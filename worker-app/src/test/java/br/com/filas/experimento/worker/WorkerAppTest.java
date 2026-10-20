package br.com.filas.experimento.worker;

import br.com.filas.experimento.adapter.local.TraceCsv;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.Trace;
import br.com.filas.experimento.domain.TraceEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Ponta a ponta com o backend local: configuração → Wiring → replay do trace → workers → JSONL. */
class WorkerAppTest {

    private static final Pattern ENQUEUE = Pattern.compile("\"enqueue_ts_us\":(\\d+)");

    /** Entradas: {offset_ms, classe (0 = PRIORITY), size_ms, slice_count}. */
    private static Map<String, String> config(Path dir, String mode, int threads, int[]... entries) throws Exception {
        List<TraceEntry> list = new ArrayList<>();
        for (int i = 0; i < entries.length; i++) {
            int[] e = entries[i];
            list.add(new TraceEntry(Duration.ofMillis(e[0]), new UUID(0, i + 1),
                    e[1] == 0 ? JobClass.PRIORITY : JobClass.NORMAL, Duration.ofMillis(e[2]), e[3]));
        }
        Path traceFile = dir.resolve("trace.csv");
        TraceCsv.write(new Trace("teste-1", 1, Map.of(), list), traceFile);

        Map<String, String> m = new HashMap<>();
        m.put("worker.id", "teste");
        m.put("worker.threads", String.valueOf(threads));
        m.put("worker.policy", "PRIORIDADE");
        m.put("worker.queues", "prio,normal");
        m.put("service.mode", mode);
        m.put("queue.wait-ms", "5");
        m.put("queue.wait-ms.prio", "0");
        m.put("sink.file", dir.resolve("out/r.jsonl").toString());
        m.put("sink.flush-ms", "20");
        m.put("local.trace", traceFile.toString());
        return m;
    }

    private static List<String> records(Map<String, String> m) throws Exception {
        return Files.readAllLines(Path.of(m.get("sink.file")));
    }

    private static long enqueueUs(String line) {
        Matcher x = ENQUEUE.matcher(line);
        assertTrue(x.find(), line);
        return Long.parseLong(x.group(1));
    }

    private static void runUntilDrained(Map<String, String> m) throws Exception {
        m.put("local.exit-when-drained", "true");
        WorkerRuntime rt = Wiring.build(WorkerConfig.from(m));
        rt.start();
        rt.awaitTermination();
        rt.close();
    }

    @Test
    void esvaziaOTraceComFatiasEPrioridadeEPara(@TempDir Path dir) throws Exception {
        Map<String, String> m = config(dir, "CPU_BURN", 1,
                new int[] {0, 1, 4, 1}, new int[] {0, 1, 6, 3}, new int[] {0, 0, 1, 1}, new int[] {0, 0, 1, 1});

        runUntilDrained(m);

        List<String> lines = records(m);
        assertEquals(6, lines.size(), "2 prioritários + 1 normal + 3 fatias");
        assertTrue(lines.stream().filter(l -> l.contains("\"class\":\"PRIORITY\"")).count() == 2);
        assertEquals(3, lines.stream().filter(l -> l.contains("\"slice_count\":3")).count());
        assertTrue(lines.stream().allMatch(l -> l.contains("\"worker_id\":\"teste/t0\"")
                && l.contains("\"run_id\":\"teste-1\"") && l.contains("\"policy\":\"PRIORIDADE\"")));
    }

    @Test
    void chegadasRespeitamOsOffsetsDoTrace(@TempDir Path dir) throws Exception {
        Map<String, String> m = config(dir, "CPU_BURN", 2,
                new int[] {0, 1, 5, 1}, new int[] {150, 0, 5, 1}, new int[] {400, 1, 5, 1});

        runUntilDrained(m);

        List<Long> ts = records(m).stream().map(WorkerAppTest::enqueueUs).sorted().toList();
        assertEquals(3, ts.size());
        assertEquals(150_000, ts.get(1) - ts.get(0), 5_000);
        assertEquals(400_000, ts.get(2) - ts.get(0), 5_000);
    }

    @Test
    void paradaGraciosaTerminaOJobEmCurso(@TempDir Path dir) throws Exception {
        Map<String, String> m = config(dir, "SLEEP", 1,
                new int[] {0, 1, 400, 1}, new int[] {0, 1, 400, 1}, new int[] {0, 1, 400, 1});
        WorkerRuntime rt = Wiring.build(WorkerConfig.from(m));
        rt.start();
        Thread.sleep(100);

        assertTrue(rt.stop(Duration.ofSeconds(5)));
        assertFalse(rt.isRunning());
        rt.close();

        assertEquals(1, records(m).size());
    }

    @Test
    void paradaNaoEsperaChegadasFuturas(@TempDir Path dir) throws Exception {
        Map<String, String> m = config(dir, "CPU_BURN", 1, new int[] {0, 1, 1, 1}, new int[] {60_000, 1, 1, 1});
        WorkerRuntime rt = Wiring.build(WorkerConfig.from(m));
        rt.start();
        Thread.sleep(100);

        long t0 = System.nanoTime();
        assertTrue(rt.stop(Duration.ofSeconds(5)));
        assertTrue(System.nanoTime() - t0 < Duration.ofSeconds(1).toNanos());
        rt.close();

        assertEquals(1, records(m).size());
    }

    @Test
    void prazoEsgotadoInterrompeSemRegistrar(@TempDir Path dir) throws Exception {
        Map<String, String> m = config(dir, "CPU_BURN", 2, new int[] {0, 1, 20_000, 1});
        WorkerRuntime rt = Wiring.build(WorkerConfig.from(m));
        rt.start();
        Thread.sleep(100);

        assertFalse(rt.stop(Duration.ofMillis(100)));
        assertFalse(rt.isRunning());
        rt.close();

        assertTrue(records(m).isEmpty());
    }

    @Test
    void rejeitaFatiaMaiorQueOVisibilityTimeout(@TempDir Path dir) throws Exception {
        Map<String, String> m = config(dir, "SLEEP", 1, new int[] {0, 1, 2_000, 1});
        m.put("queue.visibility-timeout-ms", "1000");
        assertThrows(IllegalArgumentException.class, () -> Wiring.build(WorkerConfig.from(m)));

        Map<String, String> fatiado = config(dir, "SLEEP", 1, new int[] {0, 1, 2_000, 4});
        fatiado.put("queue.visibility-timeout-ms", "1000");
        Wiring.build(WorkerConfig.from(fatiado)).close();
    }

    @Test
    void backendSqsNaoAceitaTraceLocal(@TempDir Path dir) throws Exception {
        Map<String, String> m = config(dir, "SLEEP", 1);
        m.put("queue.backend", "SQS");
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.from(m));
    }
}
