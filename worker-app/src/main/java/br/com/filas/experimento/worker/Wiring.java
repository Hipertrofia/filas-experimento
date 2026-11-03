package br.com.filas.experimento.worker;

import br.com.filas.experimento.adapter.local.BufferedJsonLinesSink;
import br.com.filas.experimento.adapter.local.CpuBurnServiceExecutor;
import br.com.filas.experimento.adapter.local.InMemoryJobQueue;
import br.com.filas.experimento.adapter.local.PreciseSleeper;
import br.com.filas.experimento.adapter.local.SleepServiceExecutor;
import br.com.filas.experimento.adapter.local.SystemClockAdapter;
import br.com.filas.experimento.adapter.local.TraceCsv;
import br.com.filas.experimento.adapter.rabbitmq.RabbitBroker;
import br.com.filas.experimento.adapter.rabbitmq.RabbitJobQueue;
import br.com.filas.experimento.adapter.sqs.SqsJobQueue;
import br.com.filas.experimento.application.TraceReplayer;
import br.com.filas.experimento.application.WorkerLoop;
import br.com.filas.experimento.application.port.in.RunWorker;
import br.com.filas.experimento.application.port.out.Clock;
import br.com.filas.experimento.application.port.out.JobQueue;
import br.com.filas.experimento.application.port.out.RecordSink;
import br.com.filas.experimento.application.port.out.ServiceExecutor;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.Trace;
import br.com.filas.experimento.domain.TraceEntry;
import br.com.filas.experimento.domain.WorkerSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/** Único ponto que conhece os adaptadores: monta o {@link WorkerRuntime} a partir da configuração. */
public final class Wiring {

    private static final long NO_RECORD_YET = Long.MIN_VALUE;

    private Wiring() {
    }

    public static WorkerRuntime build(WorkerConfig cfg) throws IOException {
        Clock clock = new SystemClockAdapter();
        if (cfg.backend() == WorkerConfig.Backend.SQS) {
            SqsJobQueue sqs = new SqsJobQueue(SqsJobQueue.client(cfg.sqsEndpoint(), cfg.sqsRegion()),
                    new SqsJobQueue.Settings(cfg.waitTimes(), cfg.defaultWait(), cfg.visibilityTimeout()));
            try {
                sqs.resolve(cfg.queues());
            } catch (RuntimeException e) {
                sqs.close();
                throw e;
            }
            AutoCloseable closeAndReport = () -> {
                sqs.receiveStats().forEach((q, s) -> System.err.printf(
                        "[sqs] %s: %d receives, %d vazios%n", q, s.receives(), s.empty()));
                sqs.close();
            };
            return assemble(cfg, id -> sqs, clock, List.of(), null, closeAndReport);
        }
        if (cfg.backend() == WorkerConfig.Backend.RABBITMQ) {
            return rabbit(cfg, clock);
        }
        InMemoryJobQueue queue = new InMemoryJobQueue(cfg.waitTimes(), cfg.defaultWait(), cfg.visibilityTimeout());

        List<RunWorker> feeders = new ArrayList<>();
        LocalFeeder feeder = null;
        if (cfg.localTrace().isPresent()) {
            Trace trace = TraceCsv.read(cfg.localTrace().get());
            for (TraceEntry e : trace.entries()) {
                if (e.sliceSize().compareTo(cfg.visibilityTimeout()) >= 0) {
                    throw new IllegalArgumentException("job " + e.jobId() + " com serviço " + e.sliceSize()
                            + " não cabe no visibility timeout " + cfg.visibilityTimeout());
                }
            }
            PreciseSleeper sleeper = cfg.localSpinMargin().map(PreciseSleeper::new).orElseGet(PreciseSleeper::forPlatform);
            feeder = new LocalFeeder(new TraceReplayer(queue, clock, sleeper, cfg.localQueueByClass()), trace);
            feeders.add(feeder);
        }

        BooleanSupplier drained = null;
        if (cfg.exitWhenDrained()) {
            List<QueueId> all = new ArrayList<>(cfg.queues());
            cfg.localQueueByClass().values().stream().filter(q -> !all.contains(q)).forEach(all::add);
            LocalFeeder f = feeder;
            drained = () -> (f == null || f.finished())
                    && queue.inFlightCount() == 0
                    && all.stream().allMatch(q -> queue.readyCount(q) == 0);
        }
        return assemble(cfg, id -> queue, clock, feeders, drained, () -> { });
    }

    /** Uma conexão por processo e um canal por worker: o prefetch = 1 vale por thread. */
    private static WorkerRuntime rabbit(WorkerConfig cfg, Clock clock) throws IOException {
        RabbitBroker broker = RabbitBroker.connect(cfg.rabbitUri(), cfg.rabbitPriorityQueues(), cfg.workerId());
        List<RabbitJobQueue> channels = new ArrayList<>();
        try {
            broker.declare(cfg.queues());
            QueueId queue = cfg.queues().get(0);
            AutoCloseable closeAll = () -> {
                try {
                    channels.forEach(RabbitJobQueue::close);
                } finally {
                    broker.close();
                }
            };
            return assemble(cfg, id -> {
                RabbitJobQueue q = broker.consumer(queue, cfg.defaultWait());
                channels.add(q);
                return q;
            }, clock, List.of(), null, closeAll);
        } catch (RuntimeException | IOException e) {
            broker.close();
            throw e;
        }
    }

    /** @param queueFor fila de cada worker, pelo id da thread */
    private static WorkerRuntime assemble(WorkerConfig cfg, Function<String, JobQueue> queueFor, Clock clock,
                                          List<RunWorker> feeders, BooleanSupplier drained,
                                          AutoCloseable queueResource) throws IOException {
        BufferedJsonLinesSink jsonl = new BufferedJsonLinesSink(cfg.sinkFile(), cfg.flushInterval());
        // Instante do último registro (monotônico), para a parada por ociosidade. A contagem só
        // começa no primeiro registro: antes disso o worker pode estar esperando a rodada começar.
        AtomicLong lastRecord = new AtomicLong(NO_RECORD_YET);
        RecordSink sink = r -> {
            jsonl.append(r);
            lastRecord.set(System.nanoTime());
        };
        if (!cfg.exitAfterIdle().isZero()) {
            long idleNanos = cfg.exitAfterIdle().toNanos();
            BooleanSupplier previous = drained;
            drained = () -> (previous == null || previous.getAsBoolean())
                    && lastRecord.get() != NO_RECORD_YET
                    && System.nanoTime() - lastRecord.get() > idleNanos;
        }
        List<RunWorker> workers = new ArrayList<>();
        for (String id : cfg.threadIds()) {
            workers.add(new WorkerLoop(new WorkerSettings(id, cfg.policy(), cfg.queues()),
                    queueFor.apply(id), executor(cfg.serviceMode()), clock, sink, PreciseSleeper.forPlatform(), cfg.idlePause()));
        }
        AutoCloseable resources = () -> {
            try {
                jsonl.close();
            } finally {
                queueResource.close();
            }
        };
        return new WorkerRuntime(workers, cfg.threadIds(), feeders, resources, drained);
    }

    private static ServiceExecutor executor(WorkerConfig.ServiceMode mode) {
        return switch (mode) {
            case SLEEP -> new SleepServiceExecutor();
            case CPU_BURN -> new CpuBurnServiceExecutor();
        };
    }
}
