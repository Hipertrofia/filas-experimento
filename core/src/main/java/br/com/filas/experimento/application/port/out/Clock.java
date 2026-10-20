package br.com.filas.experimento.application.port.out;

import java.time.Instant;

public interface Clock {

    /** Relógio de parede (NTP), para instantes comparáveis entre pods. */
    Instant wallNow();

    /** Relógio monotônico, só para durações. */
    long monotonicNanos();
}
