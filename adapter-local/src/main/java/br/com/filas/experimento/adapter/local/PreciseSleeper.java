package br.com.filas.experimento.adapter.local;

import br.com.filas.experimento.application.port.out.Sleeper;

import java.time.Duration;
import java.util.Locale;

/**
 * Espera precisa para o envio das chegadas: dorme até perto do prazo e completa em laço.
 * A margem de laço tem de cobrir o erro do sleep da plataforma (~1 ms no Linux, ~16 ms no
 * Windows pela granularidade do timer); quanto maior, mais CPU o gerador gasta.
 */
public final class PreciseSleeper implements Sleeper {

    private final long spinNanos;

    public PreciseSleeper(Duration spinMargin) {
        this.spinNanos = spinMargin.toNanos();
    }

    /** Margem padrão para o sistema operacional em uso. */
    public static PreciseSleeper forPlatform() {
        boolean windows = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
        return new PreciseSleeper(Duration.ofMillis(windows ? 16 : 2));
    }

    @Override
    public void sleepUntil(long monotonicNanos) throws InterruptedException {
        long remaining;
        while ((remaining = monotonicNanos - System.nanoTime()) > spinNanos) {
            long s = remaining - spinNanos;
            Thread.sleep(s / 1_000_000, (int) (s % 1_000_000));
        }
        while (monotonicNanos - System.nanoTime() > 0) {
            if (Thread.interrupted()) {
                throw new InterruptedException("espera interrompida");
            }
            Thread.onSpinWait();
        }
    }
}
