package br.com.filas.experimento.worker;

import br.com.filas.experimento.adapter.sqs.SqsJobQueue;
import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.QueueId;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** worker-app com queue.backend=SQS contra um ElasticMQ embutido. */
class WorkerSqsTest {

    private static SQSRestServer server;
    private static URI endpoint;
    private static SqsClient client;

    @BeforeAll
    static void start() {
        server = SQSRestServerBuilder.withDynamicPort().start();
        endpoint = URI.create("http://localhost:" + server.waitUntilStarted().localAddress().getPort());
        client = SqsJobQueue.client(Optional.of(endpoint), "us-east-1");
    }

    @AfterAll
    static void stop() {
        client.close();
        server.stopAndWait();
    }

    private static String createQueue(String prefix) {
        String name = prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
        client.createQueue(b -> b.queueName(name));
        return name;
    }

    private static Map<String, String> config(Path dir, String prio, String normal) {
        Map<String, String> m = new HashMap<>();
        m.put("worker.id", "sqs");
        m.put("worker.threads", "2");
        m.put("worker.policy", "PRIORIDADE");
        m.put("worker.queues", prio + "," + normal);
        m.put("service.mode", "CPU_BURN");
        m.put("queue.backend", "SQS");
        m.put("queue.wait-ms", "1000");
        m.put("queue.wait-ms." + prio, "0");
        m.put("queue.visibility-timeout-ms", "30000");
        m.put("sqs.endpoint", endpoint.toString());
        m.put("sink.file", dir.resolve("r.jsonl").toString());
        m.put("sink.flush-ms", "20");
        return m;
    }

    @Test
    void atendeOsJobsDoSqsEGravaOsRegistros(@TempDir Path dir) throws Exception {
        String prio = createQueue("prio");
        String normal = createQueue("normal");
        try (SqsJobQueue producer = new SqsJobQueue(SqsJobQueue.client(Optional.of(endpoint), "us-east-1"),
                new SqsJobQueue.Settings(Map.of(), Duration.ZERO, Duration.ofSeconds(30)))) {
            for (int i = 0; i < 10; i++) {
                JobClass c = i % 2 == 0 ? JobClass.PRIORITY : JobClass.NORMAL;
                Job j = Job.whole(new UUID(0, i + 1), "sqs-1", c, Duration.ofMillis(5), Instant.now());
                producer.send(j, new QueueId(c == JobClass.PRIORITY ? prio : normal));
            }
        }
        Map<String, String> m = config(dir, prio, normal);
        WorkerRuntime rt = Wiring.build(WorkerConfig.from(m));
        rt.start();

        Path out = Path.of(m.get("sink.file"));
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while ((!Files.exists(out) || Files.readAllLines(out).size() < 10) && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        // Espera do long polling da normal (1 s) + folga.
        assertTrue(rt.stop(Duration.ofSeconds(5)));
        rt.close();

        List<String> lines = Files.readAllLines(out);
        assertEquals(10, lines.size());
        assertTrue(lines.stream().allMatch(l -> l.contains("\"receive_count\":1") && l.contains("\"run_id\":\"sqs-1\"")));
        assertTrue(rt.failure().isEmpty());
        assertEquals(0, client.receiveMessage(b -> b.queueUrl(client.getQueueUrl(q -> q.queueName(normal)).queueUrl())
                .waitTimeSeconds(0)).messages().size(), "tudo confirmado");
    }

    @Test
    void ociosidadeSoContaDepoisDoPrimeiroJob(@TempDir Path dir) throws Exception {
        String prio = createQueue("prio");
        String normal = createQueue("normal");
        Map<String, String> m = config(dir, prio, normal);
        m.put("queue.wait-ms", "0");
        m.put("worker.idle-pause-ms", "5");
        m.put("worker.exit-after-idle-ms", "1000");
        WorkerRuntime rt = Wiring.build(WorkerConfig.from(m));
        rt.start();

        // A rodada só começa depois de 2 s, mais que o limite de ociosidade.
        Thread.sleep(2_000);
        assertTrue(rt.isRunning(), "não pode parar antes de atender o primeiro job");
        long t0 = System.nanoTime();
        try (SqsJobQueue producer = new SqsJobQueue(SqsJobQueue.client(Optional.of(endpoint), "us-east-1"),
                new SqsJobQueue.Settings(Map.of(), Duration.ZERO, Duration.ofSeconds(30)))) {
            producer.send(Job.whole(new UUID(0, 1), "idle", JobClass.NORMAL, Duration.ofMillis(5), Instant.now()),
                    new QueueId(normal));
        }
        rt.awaitTermination();
        rt.close();
        long elapsed = System.nanoTime() - t0;

        assertEquals(1, Files.readAllLines(Path.of(m.get("sink.file"))).size());
        assertTrue(elapsed >= Duration.ofMillis(1000).toNanos() && elapsed < Duration.ofSeconds(4).toNanos(),
                "parou ~1 s após o último job: " + elapsed / 1_000_000 + " ms");
    }

    @Test
    void filaInexistenteFalhaNaPartida(@TempDir Path dir) {
        Map<String, String> m = config(dir, "nao-existe-prio", "nao-existe-normal");
        assertThrows(QueueDoesNotExistException.class, () -> Wiring.build(WorkerConfig.from(m)));
    }

    @Test
    void esperaForaDoFormatoDoSqsFalhaNaPartida(@TempDir Path dir) {
        Map<String, String> m = config(dir, createQueue("p"), createQueue("n"));
        m.put("queue.wait-ms", "20");
        assertThrows(IllegalArgumentException.class, () -> Wiring.build(WorkerConfig.from(m)));
    }
}
