package br.com.filas.experimento.application;

import br.com.filas.experimento.application.port.in.RunWorker;
import br.com.filas.experimento.application.port.out.Clock;
import br.com.filas.experimento.application.port.out.JobQueue;
import br.com.filas.experimento.application.port.out.RecordSink;
import br.com.filas.experimento.application.port.out.ServiceExecutor;
import br.com.filas.experimento.application.port.out.Sleeper;
import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.JobRecord;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.ReceivedMessage;
import br.com.filas.experimento.domain.WorkerSettings;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Ciclo do worker (spec §3): um job por vez, filas consultadas na ordem configurada,
 * confirmação só depois de atender.
 */
public final class WorkerLoop implements RunWorker {

    private final WorkerSettings settings;
    private final JobQueue queue;
    private final ServiceExecutor executor;
    private final Clock clock;
    private final RecordSink sink;
    private final Sleeper sleeper;
    private final long idlePauseNanos;

    private volatile boolean stopRequested;

    public WorkerLoop(
            WorkerSettings settings,
            JobQueue queue,
            ServiceExecutor executor,
            Clock clock,
            RecordSink sink) {
        this(settings, queue, executor, clock, sink, deadline -> { }, Duration.ZERO);
    }

    /**
     * @param idlePause pausa depois de uma varredura em que todas as filas responderam vazio.
     *                  Com polling curto em todas as filas, limita a taxa de requisições; o
     *                  custo é até {@code idlePause} de atraso para a próxima chegada.
     */
    public WorkerLoop(
            WorkerSettings settings,
            JobQueue queue,
            ServiceExecutor executor,
            Clock clock,
            RecordSink sink,
            Sleeper sleeper,
            Duration idlePause) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
        if (idlePause.isNegative()) {
            throw new IllegalArgumentException("idlePause negativo: " + idlePause);
        }
        this.idlePauseNanos = idlePause.toNanos();
    }

    @Override
    public void run() {
        while (!stopRequested && !Thread.currentThread().isInterrupted()) {
            Optional<ReceivedMessage> next = receiveFirstAvailable();
            try {
                if (next.isEmpty()) {
                    if (idlePauseNanos > 0 && !stopRequested) {
                        sleeper.sleepUntil(clock.monotonicNanos() + idlePauseNanos);
                    }
                    continue;
                }
                serve(next.get());
            } catch (InterruptedException e) {
                // Sai sem confirmar: a mensagem volta para a fila após o visibility timeout.
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void requestStop() {
        stopRequested = true;
    }

    /** Varre as filas sempre a partir da primeira, para a de maior preferência não esperar. */
    private Optional<ReceivedMessage> receiveFirstAvailable() {
        for (QueueId q : settings.queues()) {
            Optional<ReceivedMessage> msg = queue.receive(q);
            if (msg.isPresent()) {
                return msg;
            }
            if (stopRequested) {
                break;
            }
        }
        return Optional.empty();
    }

    private void serve(ReceivedMessage msg) throws InterruptedException {
        Job job = msg.job();
        Instant receiveTs = clock.wallNow();
        Instant startTs = clock.wallNow();
        long t0 = clock.monotonicNanos();
        executor.serve(job.size());
        long t1 = clock.monotonicNanos();
        Instant endTs = clock.wallNow();

        // Próxima fatia antes do ack: se o pod cair entre os dois, sobra uma duplicata
        // (a análise deduplica), mas nenhuma fatia se perde.
        if (job.hasNextSlice()) {
            queue.send(job.nextSlice(clock.wallNow()), msg.queue());
        }
        queue.acknowledge(msg);
        sink.append(JobRecord.of(msg, settings, receiveTs, startTs, endTs, Duration.ofNanos(t1 - t0)));
    }
}
