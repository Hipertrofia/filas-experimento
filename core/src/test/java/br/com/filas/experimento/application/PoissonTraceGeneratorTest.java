package br.com.filas.experimento.application;

import br.com.filas.experimento.domain.ClassSpec;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.Trace;
import br.com.filas.experimento.domain.TraceEntry;
import br.com.filas.experimento.domain.TraceSpec;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PoissonTraceGeneratorTest {

    private static TraceSpec spec(long seed, double ratePrio, double rateNormal, Duration horizon) {
        return new TraceSpec("run", seed, horizon, Map.of(
                JobClass.PRIORITY, new ClassSpec(ratePrio, Duration.ofMillis(20), 1),
                JobClass.NORMAL, new ClassSpec(rateNormal, Duration.ofMillis(200), 1)));
    }

    private static List<TraceEntry> of(Trace t, JobClass c) {
        return t.entries().stream().filter(e -> e.jobClass() == c).toList();
    }

    @Test
    void mesmaSeedMesmoTrace() {
        TraceSpec s = spec(42, 50, 5, Duration.ofSeconds(60));
        assertEquals(PoissonTraceGenerator.generate(s), PoissonTraceGenerator.generate(s));
        assertNotEquals(PoissonTraceGenerator.generate(s).entries(),
                PoissonTraceGenerator.generate(spec(43, 50, 5, Duration.ofSeconds(60))).entries());
    }

    @Test
    void ordenadoDentroDoHorizonteComIdsUnicosEMicrossegundos() {
        Trace t = PoissonTraceGenerator.generate(spec(1, 50, 5, Duration.ofSeconds(60)));

        for (int i = 1; i < t.entries().size(); i++) {
            assertTrue(t.entries().get(i).offset().compareTo(t.entries().get(i - 1).offset()) >= 0);
        }
        assertTrue(t.entries().stream().allMatch(e -> e.offset().compareTo(Duration.ofSeconds(60)) < 0));
        assertEquals(t.entries().size(), new HashSet<>(t.entries().stream().map(TraceEntry::jobId).toList()).size());
        assertTrue(t.entries().stream().allMatch(e -> e.offset().getNano() % 1_000 == 0 && e.size().getNano() % 1_000 == 0));
    }

    @Test
    void taxaMediaEVariabilidadeBatemComPoissonExponencial() {
        double rate = 100;
        Trace t = PoissonTraceGenerator.generate(spec(7, rate, 10, Duration.ofSeconds(2_000)));
        List<TraceEntry> p = of(t, JobClass.PRIORITY);

        // ~200 mil chegadas: erro padrão relativo ~0,2%, tolerância de 1%.
        assertEquals(rate * 2_000, p.size(), rate * 2_000 * 0.01);

        double[] gaps = new double[p.size() - 1];
        for (int i = 1; i < p.size(); i++) {
            gaps[i - 1] = (p.get(i).offset().toNanos() - p.get(i - 1).offset().toNanos()) / 1e9;
        }
        double mean = java.util.Arrays.stream(gaps).average().orElseThrow();
        double var = java.util.Arrays.stream(gaps).map(g -> (g - mean) * (g - mean)).sum() / (gaps.length - 1);
        assertEquals(1 / rate, mean, 0.01 / rate);
        assertEquals(1.0, Math.sqrt(var) / mean, 0.02, "CV do intervalo entre chegadas = 1");

        double[] sizes = p.stream().mapToDouble(e -> e.size().toNanos() / 1e6).toArray();
        double ms = java.util.Arrays.stream(sizes).average().orElseThrow();
        double m2 = java.util.Arrays.stream(sizes).map(x -> x * x).average().orElseThrow();
        assertEquals(20.0, ms, 0.2, "E[S]");
        assertEquals(2 * 20.0 * 20.0, m2, 2 * 400 * 0.03, "E[S²] = 2·E[S]² na exponencial");
    }

    @Test
    void mudarUmaClasseNaoAlteraAOutra() {
        Trace a = PoissonTraceGenerator.generate(spec(5, 50, 5, Duration.ofSeconds(30)));
        Trace b = PoissonTraceGenerator.generate(spec(5, 50, 9, Duration.ofSeconds(30)));
        assertEquals(of(a, JobClass.PRIORITY), of(b, JobClass.PRIORITY));
    }

    @Test
    void mudarATaxaReescalaOsIntervalosEMantemOsTamanhos() {
        List<TraceEntry> a = of(PoissonTraceGenerator.generate(spec(5, 0.0001, 10, Duration.ofSeconds(100))), JobClass.NORMAL);
        List<TraceEntry> b = of(PoissonTraceGenerator.generate(spec(5, 0.0001, 20, Duration.ofSeconds(100))), JobClass.NORMAL);
        for (int i = 0; i < 100; i++) {
            assertEquals(a.get(i).size(), b.get(i).size());
            assertEquals(a.get(i).offset().toNanos() / 2e3, b.get(i).offset().toNanos() / 1e3, 1.0);
        }
    }

    @Test
    void taxaZeroOmiteAClasseEServicoZeroEhPermitido() {
        TraceSpec s = new TraceSpec("overhead", 1, Duration.ofSeconds(10), Map.of(
                JobClass.PRIORITY, new ClassSpec(0, Duration.ofMillis(20), 1),
                JobClass.NORMAL, new ClassSpec(20, Duration.ZERO, 1)));
        Trace t = PoissonTraceGenerator.generate(s);

        assertTrue(of(t, JobClass.PRIORITY).isEmpty());
        assertTrue(t.entries().stream().allMatch(e -> e.size().isZero()));
        assertTrue(t.entries().size() > 100);
    }
}
