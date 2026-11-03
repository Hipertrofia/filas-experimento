package br.com.filas.experimento.adapter.rabbitmq;

import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.ReceivedMessage;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Contra um RabbitMQ real (infra/rabbitmq ou RABBITMQ_URI); pulado se o broker não responder. */
class RabbitJobQueueTest {

    private static final Duration WAIT = Duration.ofSeconds(2);

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
            System.err.println("RabbitMQ fora do ar em " + uri + ": " + e);
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

    private static QueueId uniqueQueue(String prefix) {
        String name = prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
        created.add(name);
        return new QueueId(name);
    }

    private static Job job(JobClass c, int i) {
        return Job.whole(UUID.randomUUID(), "teste-" + i, c, Duration.ofMillis(i), Instant.parse("2026-11-03T12:00:00Z"));
    }

    private static long ready(QueueId q) throws Exception {
        try (Channel ch = admin.createChannel()) {
            return ch.queueDeclarePassive(q.name()).getMessageCount();
        }
    }

    @Test
    void filaComPrioridadeEntregaOPrioritarioPrimeiro() throws Exception {
        QueueId q = uniqueQueue("prio");
        try (RabbitBroker broker = RabbitBroker.connect(uri, Set.of(q), "teste")) {
            broker.declare(List.of(q));
            try (RabbitJobQueue pub = broker.publisher()) {
                pub.send(job(JobClass.NORMAL, 1), q);
                pub.send(job(JobClass.NORMAL, 2), q);
                pub.send(job(JobClass.PRIORITY, 3), q);
            }
            try (RabbitJobQueue worker = broker.consumer(q, WAIT)) {
                List<String> ordem = new ArrayList<>();
                for (int i = 0; i < 3; i++) {
                    ReceivedMessage m = worker.receive(q).orElseThrow();
                    ordem.add(m.job().runId());
                    assertEquals(q, m.queue());
                    assertEquals(1, m.receiveCount());
                    worker.acknowledge(m);
                }
                assertEquals(List.of("teste-3", "teste-1", "teste-2"), ordem);
            }
        }
    }

    @Test
    void filaSemPrioridadeEFifo() throws Exception {
        QueueId q = uniqueQueue("fifo");
        try (RabbitBroker broker = RabbitBroker.connect(uri, Set.of(), "teste")) {
            broker.declare(List.of(q));
            try (RabbitJobQueue pub = broker.publisher()) {
                pub.send(job(JobClass.NORMAL, 1), q);
                pub.send(job(JobClass.PRIORITY, 2), q);
            }
            try (RabbitJobQueue worker = broker.consumer(q, WAIT)) {
                ReceivedMessage a = worker.receive(q).orElseThrow();
                worker.acknowledge(a);
                ReceivedMessage b = worker.receive(q).orElseThrow();
                worker.acknowledge(b);
                assertEquals(List.of("teste-1", "teste-2"), List.of(a.job().runId(), b.job().runId()));
            }
        }
    }

    @Test
    void prefetchDeUmDeixaORestoNaFila() throws Exception {
        QueueId q = uniqueQueue("prefetch");
        try (RabbitBroker broker = RabbitBroker.connect(uri, Set.of(q), "teste")) {
            broker.declare(List.of(q));
            try (RabbitJobQueue pub = broker.publisher()) {
                for (int i = 0; i < 3; i++) {
                    pub.send(job(JobClass.NORMAL, i), q);
                }
            }
            try (RabbitJobQueue worker = broker.consumer(q, WAIT)) {
                ReceivedMessage m = worker.receive(q).orElseThrow();
                Thread.sleep(200);
                assertEquals(2, ready(q), "só uma entrega pode estar com o worker");
                // Enquanto não confirma, nada mais chega.
                assertTrue(worker.receive(q).isEmpty());
                worker.acknowledge(m);
                assertTrue(worker.receive(q).isPresent());
            }
        }
    }

    @Test
    void entregaNaoConfirmadaVoltaComoReentrega() throws Exception {
        QueueId q = uniqueQueue("reentrega");
        try (RabbitBroker broker = RabbitBroker.connect(uri, Set.of(q), "teste")) {
            broker.declare(List.of(q));
            Job j = job(JobClass.PRIORITY, 7);
            try (RabbitJobQueue pub = broker.publisher()) {
                pub.send(j, q);
            }
            try (RabbitJobQueue w1 = broker.consumer(q, WAIT)) {
                assertEquals(j, w1.receive(q).orElseThrow().job());
            }
            try (RabbitJobQueue w2 = broker.consumer(q, WAIT)) {
                ReceivedMessage m = w2.receive(q).orElseThrow();
                assertEquals(j, m.job());
                assertEquals(2, m.receiveCount());
                w2.acknowledge(m);
            }
            assertEquals(0, ready(q));
        }
    }

    @Test
    void corpoMalformadoERejeitadoSemTravarOCanal() throws Exception {
        QueueId q = uniqueQueue("malformado");
        try (RabbitBroker broker = RabbitBroker.connect(uri, Set.of(), "teste")) {
            broker.declare(List.of(q));
            try (Channel ch = admin.createChannel()) {
                ch.basicPublish("", q.name(), null, "não é json".getBytes(StandardCharsets.UTF_8));
            }
            try (RabbitJobQueue pub = broker.publisher()) {
                pub.send(job(JobClass.NORMAL, 1), q);
            }
            try (RabbitJobQueue worker = broker.consumer(q, WAIT)) {
                assertTrue(worker.receive(q).isEmpty());
                ReceivedMessage m = worker.receive(q).orElseThrow();
                assertEquals("teste-1", m.job().runId());
                worker.acknowledge(m);
            }
            assertEquals(0, ready(q));
        }
    }

    @Test
    void receiveVazioRespeitaAEspera() {
        QueueId q = uniqueQueue("vazia");
        try (RabbitBroker broker = RabbitBroker.connect(uri, Set.of(), "teste")) {
            broker.declare(List.of(q));
            try (RabbitJobQueue worker = broker.consumer(q, Duration.ofMillis(100))) {
                long t0 = System.nanoTime();
                assertEquals(Optional.empty(), worker.receive(q));
                long ms = (System.nanoTime() - t0) / 1_000_000;
                assertTrue(ms >= 90 && ms < 1_000, "espera de " + ms + " ms");
                assertThrows(IllegalArgumentException.class, () -> worker.receive(new QueueId("outra")));
            }
        }
    }

    @Test
    void duasFilasPorPullNaOrdemPedida() throws Exception {
        QueueId prio = uniqueQueue("pull-prio");
        QueueId normal = uniqueQueue("pull-normal");
        try (RabbitBroker broker = RabbitBroker.connect(uri, Set.of(), "teste")) {
            broker.declare(List.of(prio, normal));
            try (RabbitJobQueue pub = broker.publisher()) {
                pub.send(job(JobClass.NORMAL, 1), normal);
                pub.send(job(JobClass.PRIORITY, 2), prio);
            }
            try (RabbitJobQueue worker = broker.poller(List.of(prio, normal))) {
                ReceivedMessage p = worker.receive(prio).orElseThrow();
                assertEquals("teste-2", p.job().runId());
                assertEquals(prio, p.queue());
                worker.acknowledge(p);
                assertTrue(worker.receive(prio).isEmpty(), "basic.get vazio volta na hora");
                ReceivedMessage n = worker.receive(normal).orElseThrow();
                assertEquals(normal, n.queue());
                worker.acknowledge(n);
            }
            assertEquals(0, ready(prio) + ready(normal));
        }
    }

    @Test
    void declaracaoComArgumentosDiferentesFalha() {
        QueueId q = uniqueQueue("conflito");
        try (RabbitBroker fifo = RabbitBroker.connect(uri, Set.of(), "teste")) {
            fifo.declare(List.of(q));
        }
        try (RabbitBroker prio = RabbitBroker.connect(uri, Set.of(q), "teste")) {
            assertThrows(IllegalStateException.class, () -> prio.declare(List.of(q)));
        }
    }
}
