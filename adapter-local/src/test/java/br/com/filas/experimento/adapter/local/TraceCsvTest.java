package br.com.filas.experimento.adapter.local;

import br.com.filas.experimento.application.PoissonTraceGenerator;
import br.com.filas.experimento.domain.ClassSpec;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.Trace;
import br.com.filas.experimento.domain.TraceSpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TraceCsvTest {

    @Test
    void idaEVoltaPreservaOTrace(@TempDir Path dir) throws Exception {
        Trace t = PoissonTraceGenerator.generate(new TraceSpec("rt", 3, Duration.ofSeconds(20), Map.of(
                JobClass.PRIORITY, new ClassSpec(30, Duration.ofMillis(20), 1),
                JobClass.NORMAL, new ClassSpec(5, Duration.ofMillis(200), 4))));
        Path f = dir.resolve("sub/t.csv");

        TraceCsv.write(t, f);
        Trace back = TraceCsv.read(f);

        assertEquals(t, back);
        assertTrue(Files.readAllLines(f).get(0).equals("# run_id=rt"));
    }

    @Test
    void erroIndicaALinha(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("t.csv");
        Files.writeString(f, "# run_id=x\n# seed=1\n" + TraceCsv.COLUMNS + "\n"
                + "0,00000000-0000-0000-0000-000000000001,NORMAL,10,1\n"
                + "5,00000000-0000-0000-0000-000000000002,URGENTE,10,1\n");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> TraceCsv.read(f));
        assertTrue(e.getMessage().contains("t.csv:5"), e.getMessage());
    }

    @Test
    void exigeRunIdESeed(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("t.csv");
        Files.writeString(f, "# seed=1\n" + TraceCsv.COLUMNS + "\n");
        assertThrows(IllegalArgumentException.class, () -> TraceCsv.read(f));
    }
}
