package br.com.filas.experimento.generator;

import br.com.filas.experimento.application.JobMessageMapper;
import br.com.filas.experimento.application.TraceReplayer;
import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.Trace;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** generator-app com queue.backend=RABBITMQ contra o broker de infra/rabbitmq; pulado se ele não responder. */
class GeneratorRabbitTest {

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
    void enviaAsDuasClassesParaAFilaComPrioridade(@TempDir Path dir) throws Exception {
        String queue = "jobs-" + UUID.randomUUID().toString().substring(0, 8);
        created.add(queue);
        Map<String, String> m = new HashMap<>();
        m.put("run.id", "g-rabbit");
        m.put("seed", "4");
        m.put("horizon-s", "2");
        m.put("load.rho", "0.5");
        m.put("load.servers", "3");
        m.put("load.mix-priority", "0.5");
        m.put("trace.file", dir.resolve("t.csv").toString());
        m.put("queue.backend", "RABBITMQ");
        m.put("queue.priority", queue);
        m.put("queue.normal", queue);
        m.put("rabbitmq.uri", uri.toString());
        m.put("rabbitmq.priority-queues", queue);
        GeneratorConfig cfg = GeneratorConfig.from(m);
        Trace trace = GeneratorMain.generate(cfg);

        TraceReplayer.ReplayStats s = GeneratorMain.send(cfg);
        assertEquals(trace.entries().size(), s.sent());

        try (Channel ch = admin.createChannel()) {
            assertEquals(trace.entries().size(), ch.queueDeclarePassive(queue).getMessageCount());
            // Fila com x-max-priority: todos os prioritários saem antes do primeiro normal.
            long prio = trace.entries().stream().filter(e -> e.jobClass() == JobClass.PRIORITY).count();
            List<JobClass> ordem = new ArrayList<>();
            GetResponse r;
            while ((r = ch.basicGet(queue, true)) != null) {
                Job j = JobMessageMapper.fromBody(new String(r.getBody(), StandardCharsets.UTF_8));
                assertEquals(j.jobClass() == JobClass.PRIORITY ? 1 : 0, r.getProps().getPriority());
                ordem.add(j.jobClass());
            }
            assertEquals(trace.entries().size(), ordem.size());
            assertEquals(prio, ordem.subList(0, (int) prio).stream().filter(c -> c == JobClass.PRIORITY).count());
        }
    }
}
