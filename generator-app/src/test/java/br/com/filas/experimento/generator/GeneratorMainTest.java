package br.com.filas.experimento.generator;

import br.com.filas.experimento.adapter.local.TraceCsv;
import br.com.filas.experimento.adapter.sqs.JobMessageMapper;
import br.com.filas.experimento.adapter.sqs.SqsJobQueue;
import br.com.filas.experimento.application.TraceReplayer;
import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.Trace;
import br.com.filas.experimento.domain.TraceEntry;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeneratorMainTest {

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

    private static Map<String, String> base(Path dir, String horizon, String rho) {
        Map<String, String> m = new HashMap<>();
        m.put("run.id", "g1");
        m.put("seed", "4");
        m.put("horizon-s", horizon);
        m.put("load.rho", rho);
        m.put("load.servers", "3");
        m.put("load.mix-priority", "0.5");
        m.put("trace.file", dir.resolve("t/g1.csv").toString());
        m.put("sqs.endpoint", endpoint.toString());
        return m;
    }

    @Test
    void generateGravaUmTraceQueReproduzAUtilizacaoAlvo(@TempDir Path dir) throws Exception {
        GeneratorConfig cfg = GeneratorConfig.from(base(dir, "300", "0.7"));
        Trace written = GeneratorMain.generate(cfg);
        Trace read = TraceCsv.read(cfg.traceFile());

        assertEquals(written, read);
        assertEquals("g1", read.runId());
        TraceSummary s = TraceSummary.of(read, cfg.spec().horizon());
        // ~5700 chegadas em 300 s: ρ medido dentro de 10% do alvo.
        assertEquals(0.7, s.offeredLoad() / 3, 0.07);
        assertTrue(s.render(Optional.of(3)).contains("ρ="));
    }

    @Test
    void sendEnviaOTraceAoSqsNosOffsets(@TempDir Path dir) throws Exception {
        String prio = "gp-" + UUID.randomUUID().toString().substring(0, 8);
        String normal = "gn-" + UUID.randomUUID().toString().substring(0, 8);
        client.createQueue(b -> b.queueName(prio));
        client.createQueue(b -> b.queueName(normal));
        Map<String, String> m = base(dir, "2", "0.5");
        m.put("queue.priority", prio);
        m.put("queue.normal", normal);
        GeneratorConfig cfg = GeneratorConfig.from(m);
        Trace trace = GeneratorMain.generate(cfg);

        TraceReplayer.ReplayStats stats = GeneratorMain.send(cfg);

        assertEquals(trace.entries().size(), stats.sent());
        List<Job> fromPrio = drain(prio);
        List<Job> fromNormal = drain(normal);
        assertTrue(fromPrio.stream().allMatch(j -> j.jobClass() == JobClass.PRIORITY));
        assertTrue(fromNormal.stream().allMatch(j -> j.jobClass() == JobClass.NORMAL));
        List<Job> received = new ArrayList<>(fromPrio);
        received.addAll(fromNormal);
        assertEquals(trace.entries().size(), received.size());
        received.sort(Comparator.comparing(Job::enqueueTs));
        // Intervalos entre envios reproduzem os do trace (tolerância de 20 ms por envio no ElasticMQ).
        Job first = received.get(0);
        TraceEntry firstEntry = trace.entries().get(0);
        for (int i = 0; i < received.size(); i++) {
            Job j = received.get(i);
            TraceEntry e = trace.entries().stream().filter(x -> x.jobId().equals(j.jobId())).findFirst().orElseThrow();
            long expected = e.offset().minus(firstEntry.offset()).toNanos() / 1_000;
            long actual = java.time.Duration.between(first.enqueueTs(), j.enqueueTs()).toNanos() / 1_000;
            assertEquals(expected, actual, 20_000, "chegada " + i);
        }
    }

    @Test
    void sendRejeitaBackendLocalEJobMaiorQueOVisibility(@TempDir Path dir) throws Exception {
        Map<String, String> local = base(dir, "2", "0.5");
        local.put("queue.backend", "LOCAL");
        GeneratorMain.generate(GeneratorConfig.from(local));
        assertThrows(UnsupportedOperationException.class, () -> GeneratorMain.send(GeneratorConfig.from(local)));

        Map<String, String> curto = base(dir, "2", "0.5");
        curto.put("queue.visibility-timeout-ms", "1");
        assertThrows(IllegalArgumentException.class, () -> GeneratorMain.send(GeneratorConfig.from(curto)));
    }

    private static List<Job> drain(String queue) {
        String url = client.getQueueUrl(b -> b.queueName(queue)).queueUrl();
        List<Job> jobs = new ArrayList<>();
        while (true) {
            List<Message> msgs = client.receiveMessage(b -> b.queueUrl(url).maxNumberOfMessages(10).waitTimeSeconds(0))
                    .messages();
            if (msgs.isEmpty()) {
                return jobs;
            }
            for (Message msg : msgs) {
                jobs.add(JobMessageMapper.fromBody(msg.body()));
                client.deleteMessage(b -> b.queueUrl(url).receiptHandle(msg.receiptHandle()));
            }
        }
    }
}
