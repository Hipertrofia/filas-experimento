package br.com.filas.experimento.generator;

import br.com.filas.experimento.domain.JobClass;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GeneratorConfigTest {

    private static Map<String, String> base() {
        Map<String, String> m = new HashMap<>();
        m.put("run.id", "r1");
        m.put("seed", "9");
        m.put("horizon-s", "60");
        return m;
    }

    @Test
    void cargaPorUtilizacao() {
        Map<String, String> m = base();
        m.put("load.rho", "0.5");
        m.put("load.servers", "2");
        m.put("load.mix-priority", "0.9");
        m.put("slice.count.normal", "4");
        GeneratorConfig c = GeneratorConfig.from(m);

        // E[S] = 0,9·20 + 0,1·200 = 38 ms; λ = 0,5·2/0,038.
        double lambda = 1 / 0.038;
        assertEquals(0.9 * lambda, c.spec().classes().get(JobClass.PRIORITY).ratePerSecond(), 1e-9);
        assertEquals(0.1 * lambda, c.spec().classes().get(JobClass.NORMAL).ratePerSecond(), 1e-9);
        assertEquals(4, c.spec().classes().get(JobClass.NORMAL).sliceCount());
        assertEquals(Duration.ofSeconds(60), c.spec().horizon());
        assertEquals(Optional.of(2), c.servers());
        assertEquals(Path.of("experiments/traces/r1.csv"), c.traceFile());
        assertEquals(GeneratorConfig.Backend.SQS, c.backend());
    }

    @Test
    void taxasExplicitasParaOverhead() {
        Map<String, String> m = base();
        m.put("arrival.rate-per-s.normal", "25");
        m.put("size.mean-ms.normal", "0");
        GeneratorConfig c = GeneratorConfig.from(m);

        assertEquals(0, c.spec().classes().get(JobClass.PRIORITY).ratePerSecond());
        assertEquals(25, c.spec().classes().get(JobClass.NORMAL).ratePerSecond());
        assertEquals(Duration.ZERO, c.spec().classes().get(JobClass.NORMAL).meanSize());
    }

    @Test
    void mediasFracionariasEmMilissegundos() {
        Map<String, String> m = base();
        m.put("arrival.rate-per-s.priority", "1");
        m.put("size.mean-ms.priority", "0.5");
        assertEquals(Duration.ofNanos(500_000), GeneratorConfig.from(m).spec().classes().get(JobClass.PRIORITY).meanSize());
    }

    @Test
    void rejeitaCargaAmbigua() {
        Map<String, String> m = base();
        m.put("load.rho", "0.5");
        m.put("load.servers", "2");
        m.put("load.mix-priority", "0.5");
        m.put("arrival.rate-per-s.normal", "5");
        assertThrows(IllegalArgumentException.class, () -> GeneratorConfig.from(m));

        Map<String, String> semServers = base();
        semServers.put("load.rho", "0.5");
        semServers.put("load.mix-priority", "0.5");
        assertThrows(IllegalArgumentException.class, () -> GeneratorConfig.from(semServers));
    }

    @Test
    void systemPropertyVenceArquivo(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("g.properties");
        Files.writeString(f, "run.id=r\nseed=1\nhorizon-s=10\narrival.rate-per-s.normal=5\n");
        Properties sys = new Properties();
        sys.setProperty("seed", "77");

        assertEquals(77, GeneratorConfig.load(f, Map.of("SEED", "3"), sys).spec().seed());
        assertEquals(3, GeneratorConfig.load(f, Map.of("SEED", "3"), new Properties()).spec().seed());
    }
}
