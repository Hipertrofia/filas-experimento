package br.com.filas.experimento.application.fakes;

import br.com.filas.experimento.application.port.out.Clock;

import java.time.Duration;
import java.time.Instant;

/** Relógios avançados à mão. Parede e monotônico andam juntos, salvo ajuste explícito. */
public final class FakeClock implements Clock {

    private Instant wall;
    private long mono;

    public FakeClock(Instant start) {
        this.wall = start;
        this.mono = 1_000_000_000L;
    }

    public void advance(Duration d) {
        wall = wall.plus(d);
        mono += d.toNanos();
    }

    /** Salto só no relógio de parede, como um ajuste do NTP. */
    public void stepWall(Duration d) {
        wall = wall.plus(d);
    }

    @Override
    public Instant wallNow() {
        return wall;
    }

    @Override
    public long monotonicNanos() {
        return mono;
    }
}
