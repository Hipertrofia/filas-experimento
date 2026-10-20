package br.com.filas.experimento.application.fakes;

import br.com.filas.experimento.application.port.out.ServiceExecutor;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Avança o relógio fake pelo tamanho do job. Permite ganchos na n-ésima chamada (1-based). */
public final class FakeServiceExecutor implements ServiceExecutor {

    private final FakeClock clock;
    private final EventLog log;
    private final Map<Integer, Runnable> duringCall = new HashMap<>();
    private final Set<Integer> interruptOnCall = new HashSet<>();
    private int calls;

    public FakeServiceExecutor(FakeClock clock, EventLog log) {
        this.clock = clock;
        this.log = log;
    }

    /** Executa {@code action} durante a chamada {@code n}, depois de o relógio avançar. */
    public FakeServiceExecutor during(int n, Runnable action) {
        duringCall.put(n, action);
        return this;
    }

    public FakeServiceExecutor interruptOn(int n) {
        interruptOnCall.add(n);
        return this;
    }

    @Override
    public void serve(Duration size) throws InterruptedException {
        int n = ++calls;
        log.add("serve " + size.toMillis() + "ms");
        if (interruptOnCall.contains(n)) {
            throw new InterruptedException("interrompido no atendimento " + n);
        }
        clock.advance(size);
        duringCall.getOrDefault(n, () -> { }).run();
    }

    public int calls() {
        return calls;
    }
}
