package br.com.filas.experimento.adapter.local;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreciseSleeperTest {

    @Test
    void acordaNoPrazoComErroPequeno() throws InterruptedException {
        PreciseSleeper s = PreciseSleeper.forPlatform();
        long max = 0;
        long sum = 0;
        int n = 30;
        for (int i = 0; i < n; i++) {
            long due = System.nanoTime() + (3 + i % 7) * 1_000_000L;
            s.sleepUntil(due);
            long err = System.nanoTime() - due;
            assertTrue(err >= 0, "acordou antes do prazo: " + err);
            max = Math.max(max, err);
            sum += err;
        }
        System.out.printf("PreciseSleeper: atraso médio %d µs, máximo %d µs%n", sum / n / 1_000, max / 1_000);
        assertTrue(sum / n < 1_000_000, "atraso médio acima de 1 ms: " + sum / n);
    }

    @Test
    void prazoNoPassadoRetornaNaHora() throws InterruptedException {
        PreciseSleeper.forPlatform().sleepUntil(System.nanoTime() - 1_000_000);
    }

    @Test
    void respondeAInterrupcao() throws Exception {
        AtomicReference<Throwable> erro = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                PreciseSleeper.forPlatform().sleepUntil(System.nanoTime() + 30_000_000_000L);
            } catch (Throwable e) {
                erro.set(e);
            }
        });
        t.start();
        Thread.sleep(50);
        t.interrupt();
        t.join(2_000);

        assertTrue(!t.isAlive());
        assertInstanceOf(InterruptedException.class, erro.get());
    }
}
