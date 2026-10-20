package br.com.filas.experimento.application;

import br.com.filas.experimento.application.fakes.EventLog;
import br.com.filas.experimento.application.fakes.FakeClock;
import br.com.filas.experimento.application.fakes.FakeJobQueue;
import br.com.filas.experimento.application.fakes.FakeSleeper;
import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.Trace;
import br.com.filas.experimento.domain.TraceEntry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TraceReplayerTest {

    private static final QueueId PRIO = new QueueId("prio");
    private static final QueueId NORMAL = new QueueId("normal");
    private static final Instant T0 = Instant.parse("2026-10-20T12:00:00Z");

    private final EventLog log = new EventLog();
    private final FakeClock clock = new FakeClock(T0);
    private final FakeJobQueue queue = new FakeJobQueue(log);
    private final FakeSleeper sleeper = new FakeSleeper(clock);

    private static TraceEntry entry(int n, long offsetMs, JobClass c, int slices) {
        return new TraceEntry(Duration.ofMillis(offsetMs), new UUID(0, n), c, Duration.ofMillis(60), slices);
    }

    private static Trace trace(TraceEntry... entries) {
        return new Trace("run-7", 7, Map.of(), List.of(entries));
    }

    private TraceReplayer replayer() {
        return new TraceReplayer(queue, clock, sleeper, Map.of(JobClass.PRIORITY, PRIO, JobClass.NORMAL, NORMAL));
    }

    @Test
    void enviaCadaChegadaNoSeuOffsetParaAFilaDaClasse() throws InterruptedException {
        TraceReplayer.ReplayStats stats = replayer().replay(trace(
                entry(1, 0, JobClass.NORMAL, 1),
                entry(2, 10, JobClass.PRIORITY, 1),
                entry(3, 35, JobClass.NORMAL, 3)));

        List<FakeJobQueue.Sent> sent = queue.sent();
        assertEquals(List.of(NORMAL, PRIO, NORMAL), sent.stream().map(FakeJobQueue.Sent::queue).toList());
        assertEquals(List.of(T0, T0.plusMillis(10), T0.plusMillis(35)),
                sent.stream().map(s -> s.job().enqueueTs()).toList());

        Job sliced = sent.get(2).job();
        assertEquals("run-7", sliced.runId());
        assertEquals(Duration.ofMillis(20), sliced.size());
        assertEquals(3, sliced.sliceCount());

        assertEquals(new TraceReplayer.ReplayStats(3, 3, Duration.ZERO, Duration.ZERO), stats);
    }

    @Test
    void mantemOAgendaAbsolutaEMedeOAtraso() throws InterruptedException {
        sleeper.overshoot(Duration.ofMillis(2));

        TraceReplayer.ReplayStats stats = replayer().replay(trace(
                entry(1, 10, JobClass.NORMAL, 1),
                entry(2, 20, JobClass.NORMAL, 1)));

        // O atraso não se acumula: o segundo envio ainda é agendado para 20 ms.
        assertEquals(List.of(T0.plusMillis(12), T0.plusMillis(22)),
                queue.sent().stream().map(s -> s.job().enqueueTs()).toList());
        assertEquals(Duration.ofMillis(2), stats.maxLag());
        assertEquals(Duration.ofMillis(2), stats.meanLag());
    }

    @Test
    void paradaInterrompeOEnvio() throws InterruptedException {
        TraceReplayer r = replayer();
        r.requestStop();
        TraceReplayer.ReplayStats stats = r.replay(trace(entry(1, 0, JobClass.NORMAL, 1)));

        assertEquals(0, stats.sent());
        assertEquals(1, stats.total());
    }

    @Test
    void rejeitaClasseSemFila() {
        TraceReplayer r = new TraceReplayer(queue, clock, sleeper, Map.of(JobClass.NORMAL, NORMAL));
        assertThrows(IllegalArgumentException.class, () -> r.replay(trace(entry(1, 0, JobClass.PRIORITY, 1))));
        assertEquals(0, queue.sent().size());
    }
}
