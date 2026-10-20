package br.com.filas.experimento.application.port.out;

import java.time.Duration;

/** "Atende" o job pelo tempo nominal: sleep ou queima de CPU, conforme o adaptador. */
public interface ServiceExecutor {

    void serve(Duration size) throws InterruptedException;
}
