package br.com.filas.experimento.worker;

import br.com.filas.experimento.adapter.rabbitmq.RabbitBroker;
import br.com.filas.experimento.adapter.rabbitmq.RabbitJobQueue;
import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.QueueId;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** worker-app com queue.backend=RABBITMQ contra o broker de infra/rabbitmq; pulado se ele não responder. */
class WorkerRabbitTest {

    private static URI uri;
    private static Connection admin;
    private static final List<String> created = new ArrayList<>();

    @BeforeAll
    static void connect() {
        uri = URI.create(System.getenv().getOrDefault("RABBITMQ_URI", "amqp://guest:guest@localhost:5672"));
        try {
            ConnectionFactory f = new ConnectionFactory();
            f.setUri(uri);
            admin = f.newConnection("teste-admin");
        } catch (Exception e) {
            assumeTrue(false, "RabbitMQ fora do ar em " + uri + ": " + e);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (admin == null) {
            return;
        }
        try (Channel ch = admin.createChannel()) {
            for (String q : created) {
                ch.queueDelete(q);
            }
        }
        admin.close();
    }

    @Test
    void doisWorkersAtendemTudoComUmaEntregaPorVez(@TempDir Path dir) throws Exception {
        String queue = "jobs-" + UUID.randomUUID().toString().substring(0, 8);
        created.add(queue);
        QueueId q = new QueueId(queue);
        try (RabbitBroker broker = RabbitBroker.connect(uri, Set.of(q), "teste-produtor")) {
            broker.declare(List.of(q));
            try (RabbitJobQueue producer = broker.publisher()) {
                for (int i = 0; i < 20; i++) {
                    JobClass c = i % 2 == 0 ? JobClass.PRIORITY : JobClass.NORMAL;
                    producer.send(Job.whole(new UUID(0, i + 1), "rabbit-1", c, Duration.ofMillis(5), Instant.now()), q);
                }
            }
        }
        Map<String, String> m = new HashMap<>();
        m.put("worker.id", "rabbit");
        m.put("worker.threads", "2");
        m.put("worker.policy", "PRIORIDADE");
        m.put("worker.queues", queue);
        m.put("queue.backend", "RABBITMQ");
        m.put("queue.wait-ms", "200");
        m.put("rabbitmq.uri", uri.toString());
        m.put("rabbitmq.priority-queues", queue);
        m.put("sink.file", dir.resolve("r.jsonl").toString());
        m.put("sink.flush-ms", "20");
        WorkerRuntime rt = Wiring.build(WorkerConfig.from(m));
        rt.start();

        Path out = Path.of(m.get("sink.file"));
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while ((!Files.exists(out) || Files.readAllLines(out).size() < 20) && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(rt.stop(Duration.ofSeconds(5)));
        rt.close();

        List<String> lines = Files.readAllLines(out);
        assertEquals(20, lines.size());
        assertTrue(lines.stream().allMatch(l -> l.contains("\"receive_count\":1") && l.contains("\"queue\":\"" + queue + "\"")));
        assertEquals(2, lines.stream().map(l -> l.contains("rabbit/t0") ? "t0" : "t1").distinct().count(),
                "as duas threads atenderam");
        assertTrue(rt.failure().isEmpty());
        try (Channel ch = admin.createChannel()) {
            assertEquals(0, ch.queueDeclarePassive(queue).getMessageCount(), "tudo confirmado");
        }
    }
}
