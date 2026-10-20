package br.com.filas.experimento.worker;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Ponto de entrada. Uso: {@code worker-app [arquivo.properties]}.
 *
 * <p>SIGTERM (e Ctrl+C) chega pelo shutdown hook: pede parada graciosa, espera o job em curso
 * terminar e o sink descarregar. Passado {@code shutdown.timeout-ms}, interrompe os workers e o
 * job em curso volta para a fila sem confirmação.
 */
public final class WorkerMain {

    private WorkerMain() {
    }

    public static void main(String[] args) throws Exception {
        WorkerConfig cfg = WorkerConfig.load(args, System.getenv(), System.getProperties());
        WorkerRuntime runtime = Wiring.build(cfg);
        CountDownLatch closed = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (closed.getCount() == 0) {
                return;
            }
            log("sinal de parada: terminando o job em curso");
            try {
                if (!runtime.stop(cfg.shutdownTimeout())) {
                    log("prazo de parada esgotado: workers interrompidos");
                }
                closed.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "shutdown"));

        log("iniciando " + cfg.threadIds() + " policy=" + cfg.policy() + " queues=" + cfg.queues()
                + " mode=" + cfg.serviceMode() + " backend=" + cfg.backend() + " sink=" + cfg.sinkFile());
        runtime.start();
        runtime.awaitTermination();
        runtime.close();
        closed.countDown();
        log("parado; registros em " + cfg.sinkFile().toAbsolutePath());
        if (runtime.failure().isPresent()) {
            System.exit(1);
        }
    }

    private static void log(String msg) {
        System.err.println("[worker] " + msg);
    }
}
