package br.com.filas.experimento.worker;

import br.com.filas.experimento.application.TraceReplayer;
import br.com.filas.experimento.application.port.in.RunWorker;
import br.com.filas.experimento.domain.Trace;

/**
 * Faz o replay do trace na fila em memória, no mesmo processo dos workers (backend local:
 * a fila não é visível para outro processo). No SQS, esse papel é do generator-app.
 */
final class LocalFeeder implements RunWorker {

    private final TraceReplayer replayer;
    private final Trace trace;
    private volatile boolean finished;

    LocalFeeder(TraceReplayer replayer, Trace trace) {
        this.replayer = replayer;
        this.trace = trace;
    }

    @Override
    public void run() {
        try {
            TraceReplayer.ReplayStats s = replayer.replay(trace);
            System.err.printf("[feeder] %d/%d chegadas enviadas; atraso médio %d µs, máximo %d µs%n",
                    s.sent(), s.total(), s.meanLag().toNanos() / 1_000, s.maxLag().toNanos() / 1_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[feeder] replay interrompido");
        } finally {
            finished = true;
        }
    }

    @Override
    public void requestStop() {
        replayer.requestStop();
    }

    boolean finished() {
        return finished;
    }
}
