package br.com.filas.experimento.domain;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TraceSpecTest {

    @Test
    void taxasAPartirDaUtilizacao() {
        // c = 4, ρ = 0,8, p = 0,5: E[S] = 0,5·20 + 0,5·200 = 110 ms; λ = 0,8·4/0,110 ≈ 29,09/s.
        TraceSpec s = TraceSpec.fromUtilization("r", 1, Duration.ofSeconds(60),
                0.8, 4, 0.5, Duration.ofMillis(20), Duration.ofMillis(200), 1, 4);

        double lambda = 0.8 * 4 / 0.110;
        assertEquals(lambda / 2, s.classes().get(JobClass.PRIORITY).ratePerSecond(), 1e-9);
        assertEquals(lambda / 2, s.classes().get(JobClass.NORMAL).ratePerSecond(), 1e-9);
        assertEquals(4, s.classes().get(JobClass.NORMAL).sliceCount());
        assertEquals(0.8 * 4, s.offeredLoad(), 1e-9);
    }

    @Test
    void utilizacaoComServicoZeroExigeTaxaExplicita() {
        assertThrows(IllegalArgumentException.class, () -> TraceSpec.fromUtilization("r", 1, Duration.ofSeconds(1),
                0.5, 1, 0.5, Duration.ZERO, Duration.ZERO, 1, 1));
    }

    @Test
    void entradaDivideOServicoEntreAsFatias() {
        UUID id = new UUID(0, 1);
        TraceEntry e = new TraceEntry(Duration.ofMillis(5), id, JobClass.NORMAL, Duration.ofMillis(300), 3);
        Job j = e.firstSlice("r", Instant.EPOCH);

        assertEquals(Duration.ofMillis(100), j.size());
        assertEquals(id, j.jobId());
        assertEquals(id, j.parentJobId());
        assertEquals(0, j.sliceIndex());
        assertEquals(3, j.sliceCount());
    }

    @Test
    void traceRejeitaEntradasForaDeOrdem() {
        TraceEntry a = new TraceEntry(Duration.ofMillis(5), new UUID(0, 1), JobClass.NORMAL, Duration.ZERO, 1);
        TraceEntry b = new TraceEntry(Duration.ofMillis(4), new UUID(0, 2), JobClass.NORMAL, Duration.ZERO, 1);
        assertThrows(IllegalArgumentException.class, () -> new Trace("r", 1, Map.of(), List.of(a, b)));
    }
}
