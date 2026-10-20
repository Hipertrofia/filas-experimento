package br.com.filas.experimento.application.port.in;

public interface RunWorker {

    /** Atende jobs até {@link #requestStop()} ou interrupção da thread. Bloqueia. */
    void run();

    /** Pede parada: o job em curso termina, é confirmado e registrado; nenhum outro é recebido. */
    void requestStop();
}
