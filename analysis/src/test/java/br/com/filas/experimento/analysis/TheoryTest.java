package br.com.filas.experimento.analysis;

import br.com.filas.experimento.domain.ConsumptionPolicy;
import br.com.filas.experimento.domain.JobClass;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TheoryTest {

    private static Theory.ClassInput exponential(JobClass k, double ratePerS, double meanMs) {
        return new Theory.ClassInput(k, ratePerS / 1e3, meanMs, 2 * meanMs * meanMs, 6 * meanMs * meanMs * meanMs);
    }

    @Test
    void erlangCConfereComValorConhecido() {
        // M/M/2 com a = 1,5: C = 0,642857...
        assertEquals(0.642857, Theory.erlangC(2, 1.5), 1e-6);
        assertEquals(0.4, Theory.erlangC(1, 0.4), 1e-12);
    }

    @Test
    void fifoComUmServidorEhPollaczekKhinchine() {
        var in = List.of(exponential(JobClass.PRIORITY, 5, 20), exponential(JobClass.NORMAL, 2, 200));
        var p = Theory.predict(Theory.Model.FIFO, in, 1);
        double r2 = 5e-3 * 800 + 2e-3 * 80_000;
        double rho = 5e-3 * 20 + 2e-3 * 200;
        double wq = r2 / (2 * (1 - rho));
        assertEquals(wq, p.get(JobClass.PRIORITY).wq(), 1e-9);
        assertEquals(wq, p.get(JobClass.NORMAL).wq(), 1e-9);
        assertEquals(wq + 200, p.get(JobClass.NORMAL).w(), 1e-9);
        double r3 = 5e-3 * 6 * 8000 + 2e-3 * 6 * 8e6;
        assertEquals(2 * wq * wq + r3 / (3 * (1 - rho)), p.get(JobClass.NORMAL).wq2().getAsDouble(), 1e-6);
    }

    @Test
    void prioridadeComUmServidorEhCobham() {
        var in = List.of(exponential(JobClass.PRIORITY, 5, 20), exponential(JobClass.NORMAL, 2, 200));
        var p = Theory.predict(Theory.Model.PRIORITY, in, 1);
        double w0 = (5e-3 * 800 + 2e-3 * 80_000) / 2;
        double s1 = 0.1;
        double s2 = 0.5;
        assertEquals(w0 / (1 - s1), p.get(JobClass.PRIORITY).wq(), 1e-9);
        assertEquals(w0 / ((1 - s1) * (1 - s2)), p.get(JobClass.NORMAL).wq(), 1e-9);
    }

    @Test
    void prioridadeComUmaClasseSoReduzAFifo() {
        var in = List.of(exponential(JobClass.NORMAL, 3, 200));
        var fifo = Theory.predict(Theory.Model.FIFO, in, 1).get(JobClass.NORMAL);
        var prio = Theory.predict(Theory.Model.PRIORITY, in, 1).get(JobClass.NORMAL);
        assertEquals(fifo.wq(), prio.wq(), 1e-9);
        assertEquals(fifo.wq2().getAsDouble(), prio.wq2().getAsDouble(), 1e-6);
    }

    @Test
    void mmcComPrioridadeEMesmaMediaEhExata() {
        // M/M/c com prioridade não-preemptiva e mesma taxa de serviço: E[Wq_k] = C/(cμ(1−σ_{k−1})(1−σ_k)).
        var in = List.of(exponential(JobClass.PRIORITY, 20, 50), exponential(JobClass.NORMAL, 40, 50));
        var p = Theory.predict(Theory.Model.PRIORITY, in, 4);
        double c = Theory.erlangC(4, 3.0);
        assertEquals(c * 50 / 4 / (1 - 0.25), p.get(JobClass.PRIORITY).wq(), 1e-9);
        assertEquals(c * 50 / 4 / ((1 - 0.25) * (1 - 0.75)), p.get(JobClass.NORMAL).wq(), 1e-9);
        assertTrue(p.get(JobClass.NORMAL).wq2().isEmpty());
    }

    @Test
    void instavelNaoTemPrevisao() {
        var in = List.of(exponential(JobClass.NORMAL, 10, 200));
        assertTrue(Theory.predict(Theory.Model.FIFO, in, 2).isEmpty());
    }

    // De ponta a ponta: simulação → observações → análise → comparação com as fórmulas exatas.

    private static final Map<JobClass, QueueSimulator.ClassParams> MIX = Map.of(
            JobClass.PRIORITY, new QueueSimulator.ClassParams(2.7273, 20),
            JobClass.NORMAL, new QueueSimulator.ClassParams(2.7273, 200));

    private static RunReport simulate(Map<JobClass, QueueSimulator.ClassParams> classes, int servers,
                                      ConsumptionPolicy policy, double horizonS, long seed) {
        var obs = QueueSimulator.run(classes, servers, policy, horizonS, seed);
        return RunReport.of(Path.of("sim"), RunData.of(obs, Duration.ofSeconds(100)), Optional.of(servers),
                RunReport.ServiceBasis.MEASURED, RunReport.ModelChoice.AUTO);
    }

    private static void assertClose(double expected, double actual, double relTol, String what) {
        assertTrue(Math.abs(actual / expected - 1) < relTol,
                what + ": esperado " + expected + ", medido " + actual);
    }

    @Test
    void simulacaoFifoMG1BateComPkETakacs() {
        RunReport r = simulate(MIX, 1, ConsumptionPolicy.FIFO_COMPARTILHADO, 80_000, 11);
        assertEquals(Theory.Model.FIFO, r.model().orElseThrow());
        for (JobClass k : JobClass.values()) {
            var m = r.classes().get(k);
            var t = r.theory().get(k);
            assertClose(t.wq(), m.wq().mean(), 0.05, k + " E[Wq]");
            assertClose(t.wq2().getAsDouble(), m.wq().secondMoment(), 0.10, k + " E[Wq²]");
            assertClose(t.w2().getAsDouble(), m.w().secondMoment(), 0.10, k + " E[W²]");
        }
    }

    @Test
    void simulacaoPrioridadeMG1BateComCobhamEOSegundoMomento() {
        RunReport r = simulate(MIX, 1, ConsumptionPolicy.PRIORIDADE, 80_000, 12);
        assertEquals(Theory.Model.PRIORITY, r.model().orElseThrow());
        for (JobClass k : JobClass.values()) {
            var m = r.classes().get(k);
            var t = r.theory().get(k);
            assertClose(t.wq(), m.wq().mean(), 0.05, k + " E[Wq]");
            assertClose(t.wq2().getAsDouble(), m.wq().secondMoment(), 0.10, k + " E[Wq²]");
            assertClose(t.w2().getAsDouble(), m.w().secondMoment(), 0.10, k + " E[W²]");
        }
    }

    @Test
    void simulacaoPrioridadeMMcComMesmaMediaBateComAFormula() {
        var classes = Map.of(
                JobClass.PRIORITY, new QueueSimulator.ClassParams(10, 100),
                JobClass.NORMAL, new QueueSimulator.ClassParams(20, 100));
        RunReport r = simulate(classes, 4, ConsumptionPolicy.PRIORIDADE, 30_000, 13);
        for (JobClass k : JobClass.values()) {
            assertClose(r.theory().get(k).wq(), r.classes().get(k).wq().mean(), 0.05, k + " E[Wq]");
        }
    }
}
