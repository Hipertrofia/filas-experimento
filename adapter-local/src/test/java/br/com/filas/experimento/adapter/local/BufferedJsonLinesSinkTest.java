package br.com.filas.experimento.adapter.local;

import br.com.filas.experimento.domain.ConsumptionPolicy;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.JobRecord;
import br.com.filas.experimento.domain.QueueId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BufferedJsonLinesSinkTest {

    private static final Instant T = Instant.parse("2026-10-20T12:00:00.123456Z");

    static JobRecord record(int n, String workerId) {
        UUID id = new UUID(0, n);
        return new JobRecord(id, "run-1", JobClass.PRIORITY, Duration.ofNanos(20_345_678),
                T, T.plusMillis(1), T.plusMillis(1), T.plusMillis(21), Duration.ofNanos(20_001_999),
                workerId, ConsumptionPolicy.PRIORIDADE, new QueueId("prio"), 1, id, 0, 1);
    }

    @Test
    void serializaEmMicrossegundosComOsNomesDaSpec() {
        assertEquals("{\"job_id\":\"00000000-0000-0000-0000-000000000001\",\"run_id\":\"run-1\","
                        + "\"class\":\"PRIORITY\",\"size_us\":20345,"
                        + "\"enqueue_ts_us\":1792497600123456,\"receive_ts_us\":1792497600124456,"
                        + "\"start_ts_us\":1792497600124456,\"end_ts_us\":1792497600144456,"
                        + "\"service_actual_us\":20001,\"worker_id\":\"pod \\\"1\\\"\\\\t0\","
                        + "\"policy\":\"PRIORIDADE\",\"queue\":\"prio\",\"receive_count\":1,"
                        + "\"parent_job_id\":\"00000000-0000-0000-0000-000000000001\","
                        + "\"slice_index\":0,\"slice_count\":1}",
                JobRecordJson.toJson(record(1, "pod \"1\"\\t0")));
    }

    @Test
    void closeDescarregaTodosOsRegistrosUmPorLinha(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("rodada/registros.jsonl");
        try (BufferedJsonLinesSink sink = new BufferedJsonLinesSink(file, Duration.ofMillis(20))) {
            for (int i = 1; i <= 1000; i++) {
                sink.append(record(i, "w"));
            }
        }

        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        assertEquals(1000, lines.size());
        assertEquals(JobRecordJson.toJson(record(1, "w")), lines.get(0));
        assertEquals(JobRecordJson.toJson(record(1000, "w")), lines.get(999));
    }

    @Test
    void gravaPeriodicamenteSemPrecisarFechar(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("r.jsonl");
        try (BufferedJsonLinesSink sink = new BufferedJsonLinesSink(file, Duration.ofMillis(10))) {
            sink.append(record(1, "w"));
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            while (Files.size(file) == 0 && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertEquals(1, Files.readAllLines(file).size());
        }
    }

    @Test
    void appendDepoisDeFecharFalha(@TempDir Path dir) throws Exception {
        BufferedJsonLinesSink sink = new BufferedJsonLinesSink(dir.resolve("r.jsonl"), Duration.ofMillis(10));
        sink.close();
        assertThrows(IllegalStateException.class, () -> sink.append(record(1, "w")));
    }
}
