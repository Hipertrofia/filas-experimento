package br.com.filas.experimento.adapter.local;

import br.com.filas.experimento.application.port.out.ServiceExecutor;

import java.time.Duration;

/**
 * Modo {@code SLEEP}: dorme pelo tempo nominal. Repete o sleep até o prazo monotônico, então
 * nunca retorna antes do tempo; o atraso além do nominal aparece em {@code service_actual}.
 */
public final class SleepServiceExecutor implements ServiceExecutor {

    @Override
    public void serve(Duration size) throws InterruptedException {
        long deadline = System.nanoTime() + size.toNanos();
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > 0) {
            Thread.sleep(remaining / 1_000_000, (int) (remaining % 1_000_000));
        }
    }
}
