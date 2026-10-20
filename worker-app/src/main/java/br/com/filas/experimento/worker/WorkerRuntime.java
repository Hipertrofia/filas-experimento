package br.com.filas.experimento.worker;

import br.com.filas.experimento.application.port.in.RunWorker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * Threads de um processo e o ciclo de vida delas. Os workers param em duas etapas: pedido
 * gracioso (o job em curso termina) e, passado o prazo, interrupção (o job volta para a fila).
 * Os feeders (replay local do trace) não têm job em curso e são interrompidos de imediato.
 */
public final class WorkerRuntime implements AutoCloseable {

    private final List<RunWorker> workers;
    private final List<String> ids;
    private final List<RunWorker> feeders;
    private final List<Thread> workerThreads = new ArrayList<>();
    private final List<Thread> feederThreads = new ArrayList<>();
    private final AutoCloseable resources;
    private final BooleanSupplier drained;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    /**
     * @param drained   condição de parada automática (backend local); {@code null} para rodar até
     *                  um pedido de parada
     * @param resources fechado em {@link #close()}, depois de as threads terminarem (o sink)
     */
    WorkerRuntime(List<RunWorker> workers, List<String> ids, List<RunWorker> feeders,
                  AutoCloseable resources, BooleanSupplier drained) {
        this.workers = List.copyOf(workers);
        this.ids = List.copyOf(ids);
        this.feeders = List.copyOf(feeders);
        this.resources = resources;
        this.drained = drained;
    }

    public void start() {
        for (int i = 0; i < workers.size(); i++) {
            Thread t = new Thread(workers.get(i)::run, "worker-" + ids.get(i));
            t.setUncaughtExceptionHandler(this::onFailure);
            workerThreads.add(t);
            t.start();
        }
        for (int i = 0; i < feeders.size(); i++) {
            Thread t = new Thread(feeders.get(i)::run, "feeder-" + i);
            t.setUncaughtExceptionHandler(this::onFailure);
            feederThreads.add(t);
            t.start();
        }
    }

    public void requestStop() {
        feeders.forEach(RunWorker::requestStop);
        feederThreads.forEach(Thread::interrupt);
        workers.forEach(RunWorker::requestStop);
    }

    /** Bloqueia até todas as threads terminarem, parando sozinho se a condição de esvaziamento valer. */
    public void awaitTermination() throws InterruptedException {
        for (Thread t : workerThreads) {
            while (t.isAlive()) {
                if (drained != null && drained.getAsBoolean()) {
                    requestStop();
                }
                t.join(50);
            }
        }
        requestStop();
        for (Thread t : feederThreads) {
            t.join();
        }
    }

    /** Pede parada, espera até {@code timeout} e então interrompe quem ainda estiver atendendo. */
    public boolean stop(Duration timeout) throws InterruptedException {
        requestStop();
        long deadline = System.nanoTime() + timeout.toNanos();
        for (Thread t : allThreads()) {
            long remainingMs = Math.max(0, (deadline - System.nanoTime()) / 1_000_000);
            t.join(Math.max(1, remainingMs));
        }
        boolean graceful = allThreads().stream().noneMatch(Thread::isAlive);
        if (!graceful) {
            workerThreads.forEach(Thread::interrupt);
            for (Thread t : allThreads()) {
                t.join(5_000);
            }
        }
        return graceful;
    }

    /** Primeira exceção que derrubou uma thread; nesse caso o processo inteiro para. */
    public Optional<Throwable> failure() {
        return Optional.ofNullable(failure.get());
    }

    private void onFailure(Thread t, Throwable e) {
        failure.compareAndSet(null, e);
        System.err.println("[worker] " + t.getName() + " falhou; parando todos: " + e);
        e.printStackTrace();
        requestStop();
    }

    public boolean isRunning() {
        return allThreads().stream().anyMatch(Thread::isAlive);
    }

    @Override
    public void close() throws Exception {
        resources.close();
    }

    private List<Thread> allThreads() {
        List<Thread> all = new ArrayList<>(workerThreads);
        all.addAll(feederThreads);
        return all;
    }
}
