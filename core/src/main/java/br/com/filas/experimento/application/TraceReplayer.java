package br.com.filas.experimento.application;

import br.com.filas.experimento.application.port.out.Clock;
import br.com.filas.experimento.application.port.out.JobQueue;
import br.com.filas.experimento.application.port.out.Sleeper;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.Trace;
import br.com.filas.experimento.domain.TraceEntry;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * Envia o trace em malha aberta: cada chegada sai no seu offset, contado do início do replay,
 * independentemente do estado da fila. {@code enqueue_ts} é o instante real do envio.
 *
 * <p>O atraso de cada envio em relação ao agendado é medido; um atraso grande significa que
 * as chegadas deixaram de ser Poisson e a rodada deve ser descartada.
 */
public final class TraceReplayer {

    public record ReplayStats(int sent, int total, Duration maxLag, Duration meanLag) { }

    private final JobQueue queue;
    private final Clock clock;
    private final Sleeper sleeper;
    private final Map<JobClass, QueueId> queueByClass;

    private volatile boolean stopRequested;

    public TraceReplayer(JobQueue queue, Clock clock, Sleeper sleeper, Map<JobClass, QueueId> queueByClass) {
        this.queue = Objects.requireNonNull(queue, "queue");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
        this.queueByClass = new EnumMap<>(queueByClass);
    }

    public ReplayStats replay(Trace trace) throws InterruptedException {
        for (TraceEntry e : trace.entries()) {
            if (!queueByClass.containsKey(e.jobClass())) {
                throw new IllegalArgumentException("sem fila para a classe " + e.jobClass());
            }
        }
        long start = clock.monotonicNanos();
        int sent = 0;
        long maxLag = 0;
        long sumLag = 0;
        for (TraceEntry e : trace.entries()) {
            if (stopRequested) {
                break;
            }
            long due = start + e.offset().toNanos();
            sleeper.sleepUntil(due);
            long lag = Math.max(0, clock.monotonicNanos() - due);
            Instant enqueueTs = clock.wallNow();
            queue.send(e.firstSlice(trace.runId(), enqueueTs), queueByClass.get(e.jobClass()));
            sent++;
            maxLag = Math.max(maxLag, lag);
            sumLag += lag;
        }
        return new ReplayStats(sent, trace.entries().size(),
                Duration.ofNanos(maxLag), Duration.ofNanos(sent == 0 ? 0 : sumLag / sent));
    }

    /** Para antes da próxima chegada. Uma espera longa em curso só termina com interrupção. */
    public void requestStop() {
        stopRequested = true;
    }
}
