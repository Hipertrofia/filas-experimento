package br.com.filas.experimento.adapter.sqs;

import br.com.filas.experimento.application.WorkerLoop;
import br.com.filas.experimento.application.port.out.Clock;
import br.com.filas.experimento.application.port.out.ServiceExecutor;
import br.com.filas.experimento.domain.ConsumptionPolicy;
import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.JobRecord;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.ReceivedMessage;
import br.com.filas.experimento.domain.WorkerSettings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqsJobQueueTest {

    private static ElasticMq mq;
    private static SqsClient client;

    @BeforeAll
    static void start() {
        mq = new ElasticMq();
        client = mq.client();
    }

    @AfterAll
    static void stop() {
        client.close();
        mq.close();
    }

    private static Job job(int n, JobClass c) {
        return new Job(new UUID(0, n), "run-sqs", c, Duration.ofMillis(5),
                Instant.parse("2026-10-20T12:00:00.000123Z"), new UUID(0, n), 0, 1);
    }

    private static SqsJobQueue queue(Map<QueueId, Duration> waits, Duration defaultWait, Duration visibility) {
        // Cliente próprio por fila de teste: close() o encerra.
        return new SqsJobQueue(mq.client(), new SqsJobQueue.Settings(waits, defaultWait, visibility));
    }

    @Test
    void enviaRecebeEConfirmaUmaMensagemPorVez() {
        QueueId q = new QueueId(ElasticMq.createQueue(client, "basica"));
        try (SqsJobQueue sqs = queue(Map.of(), Duration.ofSeconds(1), Duration.ofSeconds(30))) {
            sqs.send(job(1, JobClass.NORMAL), q);
            sqs.send(job(2, JobClass.PRIORITY), q);

            ReceivedMessage m = sqs.receive(q).orElseThrow();
            assertEquals(q, m.queue());
            assertEquals(1, m.receiveCount());
            sqs.acknowledge(m);
            ReceivedMessage m2 = sqs.receive(q).orElseThrow();
            sqs.acknowledge(m2);

            assertEquals(java.util.Set.of(job(1, JobClass.NORMAL), job(2, JobClass.PRIORITY)),
                    java.util.Set.of(m.job(), m2.job()));
            assertTrue(sqs.receive(q).isEmpty());
        }
    }

    @Test
    void semAckVoltaAposOVisibilityTimeoutComReceiveCount2() {
        QueueId q = new QueueId(ElasticMq.createQueue(client, "visibilidade"));
        try (SqsJobQueue sqs = queue(Map.of(), Duration.ofSeconds(3), Duration.ofSeconds(1))) {
            sqs.send(job(1, JobClass.NORMAL), q);
            ReceivedMessage first = sqs.receive(q).orElseThrow();

            ReceivedMessage again = sqs.receive(q).orElseThrow();

            assertEquals(first.job(), again.job());
            assertEquals(2, again.receiveCount());
            sqs.acknowledge(again);
        }
    }

    @Test
    void esperaDoReceiveVemDaConfiguracaoDaFila() {
        QueueId curta = new QueueId(ElasticMq.createQueue(client, "curta"));
        QueueId longa = new QueueId(ElasticMq.createQueue(client, "longa"));
        try (SqsJobQueue sqs = queue(Map.of(curta, Duration.ZERO), Duration.ofSeconds(1), Duration.ofSeconds(30))) {
            long t0 = System.nanoTime();
            assertTrue(sqs.receive(curta).isEmpty());
            long tCurta = System.nanoTime() - t0;
            t0 = System.nanoTime();
            assertTrue(sqs.receive(longa).isEmpty());
            long tLonga = System.nanoTime() - t0;

            assertTrue(tCurta < TimeUnit.MILLISECONDS.toNanos(500), "polling curto: " + tCurta / 1_000_000 + " ms");
            assertTrue(tLonga >= TimeUnit.MILLISECONDS.toNanos(900), "long polling: " + tLonga / 1_000_000 + " ms");
        }
    }

    @Test
    void longPollingAcordaComAChegada() throws Exception {
        QueueId q = new QueueId(ElasticMq.createQueue(client, "acorda"));
        try (SqsJobQueue sqs = queue(Map.of(), Duration.ofSeconds(10), Duration.ofSeconds(30))) {
            CompletableFuture<Optional<ReceivedMessage>> waiting = CompletableFuture.supplyAsync(() -> sqs.receive(q));
            Thread.sleep(300);
            long t0 = System.nanoTime();
            sqs.send(job(1, JobClass.NORMAL), q);

            assertEquals(job(1, JobClass.NORMAL), waiting.get(5, TimeUnit.SECONDS).orElseThrow().job());
            System.out.printf("long polling: entrega %d ms após o envio%n", (System.nanoTime() - t0) / 1_000_000);
        }
    }

    @Test
    void corpoMalformadoEhIgnoradoSemDerrubarOWorker() {
        QueueId q = new QueueId(ElasticMq.createQueue(client, "malformada"));
        String url = client.getQueueUrl(b -> b.queueName(q.name())).queueUrl();
        client.sendMessage(b -> b.queueUrl(url).messageBody("{\"lixo\":1}"));
        try (SqsJobQueue sqs = queue(Map.of(), Duration.ofSeconds(1), Duration.ofSeconds(30))) {
            assertTrue(sqs.receive(q).isEmpty());
        }
    }

    /**
     * O cliente HTTP do SDK não aborta uma leitura de socket bloqueada: a interrupção só tem
     * efeito quando o long polling termina. A parada do worker pode levar até a espera configurada.
     */
    @Test
    void interrupcaoSoTemEfeitoAoFimDoLongPolling() throws Exception {
        QueueId q = new QueueId(ElasticMq.createQueue(client, "interrompe"));
        AtomicReference<Optional<ReceivedMessage>> result = new AtomicReference<>();
        AtomicReference<Boolean> flag = new AtomicReference<>();
        try (SqsJobQueue sqs = queue(Map.of(), Duration.ofSeconds(2), Duration.ofSeconds(30))) {
            Thread t = new Thread(() -> {
                result.set(sqs.receive(q));
                flag.set(Thread.currentThread().isInterrupted());
            });
            long t0 = System.nanoTime();
            t.start();
            Thread.sleep(300);
            t.interrupt();
            t.join(10_000);
            long elapsed = System.nanoTime() - t0;

            assertFalse(t.isAlive());
            System.out.printf("receive interrompido retornou após %d ms%n", elapsed / 1_000_000);
            assertTrue(elapsed < TimeUnit.SECONDS.toNanos(4), "no máximo a espera configurada + folga");
            assertTrue(result.get().isEmpty());
            assertTrue(flag.get(), "flag de interrupção preservada para o WorkerLoop sair");
        }
    }

    @Test
    void filaInexistenteFalhaNaResolucao() {
        try (SqsJobQueue sqs = queue(Map.of(), Duration.ZERO, Duration.ofSeconds(30))) {
            assertThrows(QueueDoesNotExistException.class, () -> sqs.resolve(List.of(new QueueId("nao-existe"))));
        }
    }

    @Test
    void validaEsperaEVisibilityNoFormatoDoSqs() {
        assertThrows(IllegalArgumentException.class,
                () -> new SqsJobQueue.Settings(Map.of(), Duration.ofMillis(500), Duration.ofSeconds(30)));
        assertThrows(IllegalArgumentException.class,
                () -> new SqsJobQueue.Settings(Map.of(), Duration.ofSeconds(21), Duration.ofSeconds(30)));
        assertThrows(IllegalArgumentException.class,
                () -> new SqsJobQueue.Settings(Map.of(), Duration.ZERO, Duration.ofMillis(1500)));
        assertThrows(IllegalArgumentException.class,
                () -> new SqsJobQueue.Settings(Map.of(new QueueId("p"), Duration.ofMillis(20)), Duration.ZERO,
                        Duration.ofSeconds(30)));
    }

    @Test
    void workerLoopComPrioridadeSobreSqs() {
        QueueId prio = new QueueId(ElasticMq.createQueue(client, "prio"));
        QueueId normal = new QueueId(ElasticMq.createQueue(client, "normal"));
        List<JobRecord> records = Collections.synchronizedList(new ArrayList<>());
        Clock clock = new Clock() {
            @Override
            public Instant wallNow() {
                return Instant.now();
            }

            @Override
            public long monotonicNanos() {
                return System.nanoTime();
            }
        };
        ServiceExecutor noop = size -> { };
        try (SqsJobQueue sqs = queue(Map.of(prio, Duration.ZERO), Duration.ofSeconds(1), Duration.ofSeconds(30))) {
            for (int i = 0; i < 3; i++) {
                sqs.send(job(10 + i, JobClass.NORMAL), normal);
            }
            for (int i = 0; i < 3; i++) {
                sqs.send(job(20 + i, JobClass.PRIORITY), prio);
            }
            WorkerLoop[] loop = new WorkerLoop[1];
            loop[0] = new WorkerLoop(new WorkerSettings("sqs/t0", ConsumptionPolicy.PRIORIDADE, List.of(prio, normal)),
                    sqs, noop, clock, r -> {
                        records.add(r);
                        if (records.size() == 6) {
                            loop[0].requestStop();
                        }
                    });
            loop[0].run();
        }

        assertEquals(6, records.size());
        assertTrue(records.subList(0, 3).stream().allMatch(r -> r.jobClass() == JobClass.PRIORITY),
                "prioritários primeiro: " + records.stream().map(JobRecord::jobClass).toList());
        assertTrue(records.stream().allMatch(r -> r.receiveCount() == 1 && !r.startTs().isBefore(r.receiveTs())));
    }
}
