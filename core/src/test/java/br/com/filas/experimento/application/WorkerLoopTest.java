package br.com.filas.experimento.application;

import br.com.filas.experimento.application.fakes.EventLog;
import br.com.filas.experimento.application.fakes.FakeClock;
import br.com.filas.experimento.application.fakes.FakeJobQueue;
import br.com.filas.experimento.application.fakes.FakeServiceExecutor;
import br.com.filas.experimento.application.fakes.ListRecordSink;
import br.com.filas.experimento.domain.ConsumptionPolicy;
import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.JobRecord;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.WorkerSettings;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerLoopTest {

    private static final QueueId PRIO = new QueueId("prioritaria");
    private static final QueueId NORMAL = new QueueId("normal");
    private static final QueueId UNICA = new QueueId("unica");
    private static final Instant T0 = Instant.parse("2026-10-20T12:00:00Z");

    private final EventLog log = new EventLog();
    private final FakeClock clock = new FakeClock(T0.plusSeconds(1));
    private final FakeJobQueue queue = new FakeJobQueue(log);
    private final FakeServiceExecutor executor = new FakeServiceExecutor(clock, log);
    private final ListRecordSink sink = new ListRecordSink(log);

    private static UUID id(int n) {
        return new UUID(0, n);
    }

    private static Job job(int n, JobClass jobClass, long sizeMs) {
        return Job.whole(id(n), "run-1", jobClass, Duration.ofMillis(sizeMs), T0.plusMillis(n));
    }

    private WorkerLoop loop(ConsumptionPolicy policy, QueueId... queues) {
        WorkerLoop loop = new WorkerLoop(
                new WorkerSettings("pod-1/t0", policy, List.of(queues)), queue, executor, clock, sink);
        queue.whenAllEmpty(loop::requestStop);
        return loop;
    }

    private List<UUID> servedOrder() {
        return sink.records().stream().map(JobRecord::jobId).toList();
    }

    @Test
    void prioridadeAtendeOPrioritarioAntesDoNormalQuandoAmbosEsperam() {
        queue.enqueue(NORMAL, job(1, JobClass.NORMAL, 200))
             .enqueue(PRIO, job(2, JobClass.PRIORITY, 20));

        loop(ConsumptionPolicy.PRIORIDADE, PRIO, NORMAL).run();

        assertEquals(List.of(id(2), id(1)), servedOrder());
    }

    @Test
    void prioritarioQueChegaDuranteOAtendimentoEOProximo() {
        queue.enqueue(NORMAL, job(1, JobClass.NORMAL, 200))
             .enqueue(NORMAL, job(2, JobClass.NORMAL, 200));
        executor.during(1, () -> queue.enqueue(PRIO, job(3, JobClass.PRIORITY, 20)));

        loop(ConsumptionPolicy.PRIORIDADE, PRIO, NORMAL).run();

        assertEquals(List.of(id(1), id(3), id(2)), servedOrder());
    }

    @Test
    void pausaDeOciosidadeEntreVarredurasVazias() {
        WorkerLoop loop = new WorkerLoop(
                new WorkerSettings("pod-1/t0", ConsumptionPolicy.PRIORIDADE, List.of(PRIO, NORMAL)),
                queue, executor, clock, sink, new br.com.filas.experimento.application.fakes.FakeSleeper(clock),
                Duration.ofMillis(5));
        long t0 = clock.monotonicNanos();
        int[] emptyScans = {0};
        queue.whenAllEmpty(() -> {
            // Chamado a cada receive vazio: duas filas por varredura.
            if (++emptyScans[0] == 6) {
                loop.requestStop();
            }
        });

        loop.run();

        // 3 varreduras vazias; a última pede parada e não pausa.
        assertEquals(Duration.ofMillis(10).toNanos(), clock.monotonicNanos() - t0);
        assertEquals(6, log.count("receive"));
    }

    @Test
    void fifoCompartilhadoAtendeNaOrdemDeChegada() {
        queue.enqueue(UNICA, job(1, JobClass.NORMAL, 200))
             .enqueue(UNICA, job(2, JobClass.PRIORITY, 20))
             .enqueue(UNICA, job(3, JobClass.NORMAL, 200));

        loop(ConsumptionPolicy.FIFO_COMPARTILHADO, UNICA).run();

        assertEquals(List.of(id(1), id(2), id(3)), servedOrder());
    }

    @Test
    void registroCompletoEComInstantesOrdenados() {
        Job j = job(1, JobClass.PRIORITY, 20);
        queue.enqueue(PRIO, j);

        loop(ConsumptionPolicy.PRIORIDADE, PRIO, NORMAL).run();

        JobRecord r = sink.records().get(0);
        assertEquals(j.jobId(), r.jobId());
        assertEquals("run-1", r.runId());
        assertEquals(JobClass.PRIORITY, r.jobClass());
        assertEquals(Duration.ofMillis(20), r.size());
        assertEquals(j.enqueueTs(), r.enqueueTs());
        assertFalse(r.startTs().isBefore(r.enqueueTs()), "start >= enqueue");
        assertFalse(r.startTs().isBefore(r.receiveTs()), "start >= receive");
        assertFalse(r.endTs().isBefore(r.startTs()), "end >= start");
        assertEquals(Duration.ofMillis(20), r.serviceActual());
        assertEquals(Duration.ofMillis(20), Duration.between(r.startTs(), r.endTs()));
        assertEquals("pod-1/t0", r.workerId());
        assertEquals(ConsumptionPolicy.PRIORIDADE, r.policy());
        assertEquals(PRIO, r.queue());
        assertEquals(1, r.receiveCount());
        assertEquals(j.jobId(), r.parentJobId());
        assertEquals(0, r.sliceIndex());
        assertEquals(1, r.sliceCount());
    }

    @Test
    void serviceActualUsaORelogioMonotonicoMesmoComSaltoNoRelogioDeParede() {
        queue.enqueue(UNICA, job(1, JobClass.NORMAL, 200));
        executor.during(1, () -> clock.stepWall(Duration.ofMillis(-50)));

        loop(ConsumptionPolicy.FIFO_COMPARTILHADO, UNICA).run();

        assertEquals(Duration.ofMillis(200), sink.records().get(0).serviceActual());
    }

    @Test
    void confirmaSoDepoisDeAtenderERegistraDepoisDoAck() {
        queue.enqueue(UNICA, job(1, JobClass.NORMAL, 200));

        loop(ConsumptionPolicy.FIFO_COMPARTILHADO, UNICA).run();

        List<String> e = log.events();
        int serve = e.indexOf("serve 200ms");
        int ack = e.indexOf("ack " + id(1));
        int record = e.indexOf("record " + id(1));
        assertTrue(serve >= 0 && serve < ack && ack < record, e.toString());
    }

    @Test
    void paradaDuranteOAtendimentoTerminaOJobEmCursoENaoRecebeOutro() {
        queue.enqueue(UNICA, job(1, JobClass.NORMAL, 200))
             .enqueue(UNICA, job(2, JobClass.NORMAL, 200));
        WorkerLoop loop = loop(ConsumptionPolicy.FIFO_COMPARTILHADO, UNICA);
        executor.during(1, loop::requestStop);

        loop.run();

        assertEquals(List.of(id(1)), servedOrder());
        assertEquals(1, queue.acknowledged().size());
        assertEquals(1, log.count("receive"));
    }

    @Test
    void interrupcaoNoAtendimentoSaiSemConfirmarNemRegistrar() {
        queue.enqueue(UNICA, job(1, JobClass.NORMAL, 200));
        executor.interruptOn(1);

        try {
            loop(ConsumptionPolicy.FIFO_COMPARTILHADO, UNICA).run();
            assertTrue(Thread.currentThread().isInterrupted(), "flag de interrupção restaurada");
        } finally {
            Thread.interrupted();
        }

        assertTrue(queue.acknowledged().isEmpty());
        assertTrue(sink.records().isEmpty());
        assertEquals(1, queue.inFlightCount());
    }

    @Test
    void reentregaGeraDoisRegistrosComReceiveCountCrescente() {
        Job j = job(1, JobClass.NORMAL, 200);
        queue.enqueue(UNICA, j).enqueue(UNICA, j);

        loop(ConsumptionPolicy.FIFO_COMPARTILHADO, UNICA).run();

        List<JobRecord> rs = sink.records();
        assertEquals(List.of(id(1), id(1)), servedOrder());
        assertEquals(1, rs.get(0).receiveCount());
        assertEquals(2, rs.get(1).receiveCount());
    }

    @Test
    void fatiaReenfileiraAProximaNoFimDaMesmaFila() {
        UUID parent = id(100);
        Job slice0 = new Job(parent, "run-1", JobClass.NORMAL, Duration.ofMillis(50), T0, parent, 0, 3);
        queue.enqueue(NORMAL, slice0)
             .enqueue(NORMAL, job(2, JobClass.NORMAL, 200));

        loop(ConsumptionPolicy.PRIORIDADE, PRIO, NORMAL).run();

        List<FakeJobQueue.Sent> sent = queue.sent();
        assertEquals(2, sent.size(), "fatias 1 e 2; a última não reenvia");
        Job slice1 = sent.get(0).job();
        Job slice2 = sent.get(1).job();
        assertEquals(NORMAL, sent.get(0).queue());
        assertEquals(slice0.nextSlice(T0).jobId(), slice1.jobId());
        assertEquals(parent, slice1.parentJobId());
        assertEquals(1, slice1.sliceIndex());
        assertEquals(3, slice1.sliceCount());
        assertEquals(Duration.ofMillis(50), slice1.size());
        assertEquals(2, slice2.sliceIndex());

        // Fatia 0, depois o job 2 que já esperava, depois as fatias 1 e 2 no fim da fila.
        assertEquals(List.of(parent, id(2), slice1.jobId(), slice2.jobId()), servedOrder());
    }

    @Test
    void enviaProximaFatiaAntesDeConfirmarAAtual() {
        UUID parent = id(100);
        Job slice0 = new Job(parent, "run-1", JobClass.NORMAL, Duration.ofMillis(50), T0, parent, 0, 2);
        queue.enqueue(UNICA, slice0);

        loop(ConsumptionPolicy.FIFO_COMPARTILHADO, UNICA).run();

        List<String> e = log.events();
        int send = e.indexOf("send " + UNICA + " " + slice0.nextSlice(T0).jobId());
        int ack = e.indexOf("ack " + parent);
        assertTrue(send >= 0 && send < ack, e.toString());
    }
}
