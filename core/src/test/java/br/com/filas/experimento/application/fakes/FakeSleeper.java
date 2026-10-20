package br.com.filas.experimento.application.fakes;

import br.com.filas.experimento.application.port.out.Sleeper;

import java.time.Duration;

/** Avança o relógio fake até o prazo, mais um atraso opcional (simula um sleep impreciso). */
public final class FakeSleeper implements Sleeper {

    private final FakeClock clock;
    private Duration overshoot = Duration.ZERO;

    public FakeSleeper(FakeClock clock) {
        this.clock = clock;
    }

    public FakeSleeper overshoot(Duration d) {
        this.overshoot = d;
        return this;
    }

    @Override
    public void sleepUntil(long monotonicNanos) {
        long wait = monotonicNanos - clock.monotonicNanos();
        if (wait > 0) {
            clock.advance(Duration.ofNanos(wait).plus(overshoot));
        }
    }
}
