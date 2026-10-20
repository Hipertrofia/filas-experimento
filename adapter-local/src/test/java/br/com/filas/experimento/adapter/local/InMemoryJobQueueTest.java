package br.com.filas.experimento.adapter.local;

import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.ReceivedMessage;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryJobQueueTest {

    private static final QueueId A = new QueueId("a");
    private static final QueueId B = new QueueId("b");
    private static final Duration LONG = Duration.ofMinutes(1);

    private static Job job(int n) {
        return Job.whole(new UUID(0, n), "run", JobClass.NORMAL, Duration.ofMillis(20), Instant.EPOCH);
    }

    @Test
    void entregaNaOrdemDeEnvioUmaMensagemPorVez() {
        InMemoryJobQueue q = new InMemoryJobQueue(Duration.ZERO, LONG);
        q.send(job(1), A);
        q.send(job(2), A);

        ReceivedMessage m1 = q.receive(A).orElseThrow();
        assertEquals(job(1), m1.job());
        assertEquals(A, m1.queue());
        assertEquals(1, m1.receiveCount());
        assertEquals(1, q.readyCount(A), "sem prefetch: a segunda continua na fila");
        assertEquals(job(2), q.receive(A).orElseThrow().job());
        assertTrue(q.receive(A).isEmpty());
    }

    @Test
    void filasSaoIndependentes() {
        InMemoryJobQueue q = new InMemoryJobQueue(Duration.ZERO, LONG);
        q.send(job(1), B);

        assertTrue(q.receive(A).isEmpty());
        assertEquals(job(1), q.receive(B).orElseThrow().job());
    }

    @Test
    void ackRemoveDefinitivamente() {
        // A espera do receive (100 ms) passa do visibility timeout (30 ms): sem o ack, voltaria.
        InMemoryJobQueue q = new InMemoryJobQueue(Duration.ofMillis(100), Duration.ofMillis(30));
        q.send(job(1), A);
        q.acknowledge(q.receive(A).orElseThrow());

        assertEquals(0, q.inFlightCount());
        assertTrue(q.receive(A).isEmpty());
    }

    @Test
    void semAckAMensagemVoltaAposOVisibilityTimeoutComReceiveCountIncrementado() {
        InMemoryJobQueue q = new InMemoryJobQueue(Duration.ofSeconds(2), Duration.ofMillis(50));
        q.send(job(1), A);
        ReceivedMessage first = q.receive(A).orElseThrow();

        ReceivedMessage again = q.receive(A).orElseThrow();
        assertEquals(job(1), again.job());
        assertEquals(2, again.receiveCount());

        q.acknowledge(first); // handle expirado: ignorado
        assertEquals(1, q.inFlightCount());
        q.acknowledge(again);
        assertEquals(0, q.inFlightCount());
    }

    @Test
    void receiveVazioRespeitaOTempoDeEsperaDaFila() {
        InMemoryJobQueue q = new InMemoryJobQueue(Map.of(A, Duration.ZERO), Duration.ofMillis(100), LONG);

        long t0 = System.nanoTime();
        assertTrue(q.receive(A).isEmpty());
        long curto = System.nanoTime() - t0;

        t0 = System.nanoTime();
        assertTrue(q.receive(B).isEmpty());
        long longo = System.nanoTime() - t0;

        assertTrue(curto < TimeUnit.MILLISECONDS.toNanos(50), "polling curto: " + curto);
        assertTrue(longo >= TimeUnit.MILLISECONDS.toNanos(100), "long polling: " + longo);
    }

    @Test
    void longPollingAcordaQuandoChegaMensagem() throws Exception {
        InMemoryJobQueue q = new InMemoryJobQueue(Duration.ofSeconds(10), LONG);
        CompletableFuture<Optional<ReceivedMessage>> waiting = CompletableFuture.supplyAsync(() -> q.receive(A));

        Thread.sleep(50);
        long t0 = System.nanoTime();
        q.send(job(1), A);
        Optional<ReceivedMessage> m = waiting.get(5, TimeUnit.SECONDS);

        assertEquals(job(1), m.orElseThrow().job());
        assertTrue(System.nanoTime() - t0 < TimeUnit.SECONDS.toNanos(1));
    }

    @Test
    void interrupcaoDuranteAEsperaRetornaVazioEPreservaAFlag() {
        InMemoryJobQueue q = new InMemoryJobQueue(Duration.ofSeconds(10), LONG);
        Thread.currentThread().interrupt();
        try {
            assertTrue(q.receive(A).isEmpty());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
