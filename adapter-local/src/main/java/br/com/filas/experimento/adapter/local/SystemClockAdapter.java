package br.com.filas.experimento.adapter.local;

import br.com.filas.experimento.application.port.out.Clock;

import java.time.Instant;

/** Parede: {@link Instant#now()} (o sincronismo NTP é responsabilidade do host). Monotônico: {@link System#nanoTime()}. */
public final class SystemClockAdapter implements Clock {

    @Override
    public Instant wallNow() {
        return Instant.now();
    }

    @Override
    public long monotonicNanos() {
        return System.nanoTime();
    }
}
