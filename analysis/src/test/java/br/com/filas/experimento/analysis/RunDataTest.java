package br.com.filas.experimento.analysis;

import br.com.filas.experimento.domain.ConsumptionPolicy;
import br.com.filas.experimento.domain.JobClass;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RunDataTest {

    private static final long T0 = 1_792_500_000_000_000L;

    private static Observation obs(UUID id, UUID parent, int slice, int slices, long enqueue, long start, long end) {
        return new Observation(id, "r", JobClass.NORMAL, end - start, T0 + enqueue, T0 + start, T0 + start, T0 + end,
                end - start, "w", ConsumptionPolicy.PRIORIDADE, "normal", 1, parent, slice, slices);
    }

    private static Observation whole(long n, long enqueue, long start, long end) {
        UUID id = new UUID(0, n);
        return obs(id, id, 0, 1, enqueue, start, end);
    }

    @Test
    void leALinhaGravadaPeloWorker() {
        Observation o = Observation.parse("{\"job_id\":\"6ec20ffa-6a4a-379b-8fe3-0d2d6d2377b2\",\"run_id\":\"fumaca-s1\","
                + "\"class\":\"PRIORITY\",\"size_us\":8299,\"enqueue_ts_us\":1792505549284909,"
                + "\"receive_ts_us\":1792505549301688,\"start_ts_us\":1792505549301688,\"end_ts_us\":1792505549309333,"
                + "\"service_actual_us\":8327,\"worker_id\":\"local/t1\",\"policy\":\"PRIORIDADE\","
                + "\"queue\":\"prioritaria\",\"receive_count\":1,"
                + "\"parent_job_id\":\"6ec20ffa-6a4a-379b-8fe3-0d2d6d2377b2\",\"slice_index\":0,\"slice_count\":1}");
        assertEquals(JobClass.PRIORITY, o.jobClass());
        assertEquals(1792505549301688L - 1792505549284909L, o.startUs() - o.enqueueUs());
        assertEquals("local/t1", o.workerId());
    }

    @Test
    void jsonComEscapesERejeitaLinhaQuebrada() {
        assertEquals("a\"b\\cé", JsonLine.parse("{\"k\":\"a\\\"b\\\\c\\u00e9\"}").get("k"));
        assertThrows(IllegalArgumentException.class, () -> JsonLine.parse("{\"k\":1"));
        assertThrows(IllegalArgumentException.class, () -> JsonLine.parse("{\"k\":1} x"));
    }

    @Test
    void duplicataFicaComAPrimeiraConclusao() {
        UUID id = new UUID(0, 1);
        RunData d = RunData.of(List.of(
                obs(id, id, 0, 1, 0, 50_000, 90_000),
                obs(id, id, 0, 1, 0, 10_000, 30_000)), Duration.ZERO);
        assertEquals(1, d.duplicates());
        assertEquals(1, d.jobs().size());
        assertEquals(10_000, d.jobs().get(0).waitUs());
        assertEquals(30_000, d.jobs().get(0).sojournUs());
    }

    @Test
    void fatiasViramUmJobPai() {
        UUID parent = new UUID(0, 7);
        RunData d = RunData.of(List.of(
                obs(new UUID(1, 2), parent, 2, 3, 400, 500, 600),
                obs(parent, parent, 0, 3, 0, 100, 200),
                obs(new UUID(1, 1), parent, 1, 3, 200, 250, 350)), Duration.ZERO);
        RunData.JobOutcome j = d.jobs().get(0);
        assertEquals(3, j.sliceCount());
        assertEquals(100 + 50 + 100, j.waitUs());
        assertEquals(300, j.serviceUs());
        assertEquals(600, j.sojournUs());
    }

    @Test
    void jobComFatiaFaltandoFicaDeFora() {
        UUID parent = new UUID(0, 7);
        RunData d = RunData.of(List.of(
                obs(parent, parent, 0, 2, 0, 100, 200),
                whole(9, 0, 0, 10)), Duration.ZERO);
        assertEquals(1, d.incomplete());
        assertEquals(1, d.jobs().size());
    }

    @Test
    void aquecimentoDescartaPelaChegada() {
        RunData d = RunData.of(List.of(
                whole(1, 0, 0, 10),
                whole(2, 999_999, 1_000_000, 1_000_010),
                whole(3, 1_000_000, 1_000_010, 1_000_020),
                whole(4, 3_000_000, 3_000_000, 3_000_010)), Duration.ofSeconds(1));
        assertEquals(2, d.warmupDiscarded());
        assertEquals(2, d.jobs().size());
        assertEquals(2.0, d.windowSeconds(), 1e-9);
    }

    @Test
    void contaEsperasNegativas() {
        RunData d = RunData.of(List.of(whole(1, 100, 50, 80)), Duration.ZERO);
        assertEquals(1, d.negativeWaits());
    }

    @Test
    void quantisPorPostoMaisProximo() {
        double[] v = new double[100];
        for (int i = 0; i < 100; i++) {
            v[i] = 100 - i;
        }
        Sample s = Sample.of(v);
        assertEquals(50, s.p50());
        assertEquals(99, s.p99());
        assertEquals(50.5, s.mean(), 1e-12);
    }

    @Test
    void intervaloDasReplicas() {
        Replicas.Estimate e = Replicas.estimate(new double[] {1, 2, 3});
        assertEquals(2, e.mean(), 1e-12);
        assertEquals(4.303 * 1 / Math.sqrt(3), e.halfWidth(), 1e-9);
    }
}
