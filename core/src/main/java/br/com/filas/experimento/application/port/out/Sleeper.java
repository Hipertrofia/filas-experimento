package br.com.filas.experimento.application.port.out;

/** Espera até um instante do relógio monotônico ({@link Clock#monotonicNanos()}). */
public interface Sleeper {

    void sleepUntil(long monotonicNanos) throws InterruptedException;
}
