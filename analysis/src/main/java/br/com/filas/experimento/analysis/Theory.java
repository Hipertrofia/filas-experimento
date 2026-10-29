package br.com.filas.experimento.analysis;

import br.com.filas.experimento.domain.JobClass;

import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

/**
 * Previsões para chegadas de Poisson por classe e c servidores (M/G/c), em ms.
 *
 * <ul>
 *   <li>FIFO: Pollaczek–Khinchine, com o segundo momento de Takács, ambos exatos para c = 1. Para
 *       c &gt; 1, Allen–Cunneen: E[Wq] = C(c, a)·E[S²] / (2·E[S]·c·(1 − ρ)), que é exata para M/M/c
 *       e se reduz a P-K em c = 1.</li>
 *   <li>Prioridade não-preemptiva: Cobham, exata para c = 1, com o segundo momento por classe.
 *       Para c &gt; 1, Bondi–Buzen: E[Wq_k] = E[Wq_FIFO(M/G/c)]·(1 − ρ) / ((1 − σ_{k−1})(1 − σ_k)),
 *       exata quando todas as classes têm serviço exponencial de mesma média.</li>
 * </ul>
 *
 * Os segundos momentos só existem onde a fórmula é exata (c = 1).
 */
final class Theory {

    enum Model {
        FIFO("FIFO: Pollaczek–Khinchine", "FIFO: Allen–Cunneen (aprox.)"),
        PRIORITY("prioridade não-preemptiva: Cobham", "prioridade não-preemptiva: Bondi–Buzen (aprox.)");

        private final String exact;
        private final String approx;

        Model(String exact, String approx) {
            this.exact = exact;
            this.approx = approx;
        }

        String label(int servers) {
            return servers == 1 ? exact : approx;
        }
    }

    /** Entrada por classe: λ em 1/ms e momentos do serviço em ms, ms², ms³. */
    record ClassInput(JobClass jobClass, double lambdaPerMs, double m1, double m2, double m3) { }

    record Prediction(double wq, OptionalDouble wq2, double w, OptionalDouble w2) { }

    private Theory() {
    }

    /** Previsões por classe; vazio se ρ ≥ 1. A ordem de prioridade é a de {@link JobClass}. */
    static Map<JobClass, Prediction> predict(Model model, List<ClassInput> classes, int servers) {
        if (servers < 1) {
            throw new IllegalArgumentException("servidores < 1: " + servers);
        }
        double lambda = 0;
        double r1 = 0;
        double r2 = 0;
        double r3 = 0;
        for (ClassInput k : classes) {
            lambda += k.lambdaPerMs();
            r1 += k.lambdaPerMs() * k.m1();
            r2 += k.lambdaPerMs() * k.m2();
            r3 += k.lambdaPerMs() * k.m3();
        }
        double rho = r1 / servers;
        Map<JobClass, Prediction> out = new EnumMap<>(JobClass.class);
        if (rho >= 1 || lambda <= 0) {
            return out;
        }
        double fifoWq = erlangC(servers, r1) * r2 / (2 * r1 * servers * (1 - rho));

        List<ClassInput> ordered = classes.stream()
                .sorted(Comparator.comparing(ClassInput::jobClass))
                .toList();
        double sigmaBefore = 0;
        double r2Before = 0;
        for (ClassInput k : ordered) {
            double sigma = sigmaBefore + k.lambdaPerMs() * k.m1() / servers;
            double r2UpTo = r2Before + k.lambdaPerMs() * k.m2();
            double wq;
            OptionalDouble wq2 = OptionalDouble.empty();
            if (model == Model.FIFO) {
                wq = fifoWq;
                if (servers == 1) {
                    wq2 = OptionalDouble.of(2 * wq * wq + r3 / (3 * (1 - rho)));
                }
            } else {
                double a = 1 - sigmaBefore;
                double b = 1 - sigma;
                wq = fifoWq * (1 - rho) / (a * b);
                if (servers == 1) {
                    // Segundo momento da espera na prioridade não-preemptiva M/G/1 (Takagi, vol. 1).
                    wq2 = OptionalDouble.of(r3 / (3 * a * a * b)
                            + r2 * r2UpTo / (2 * a * a * b * b)
                            + r2 * r2Before / (2 * a * a * a * b));
                }
            }
            // Na prioridade não-preemptiva, o serviço do job é independente da espera dele.
            OptionalDouble w2 = wq2.isPresent()
                    ? OptionalDouble.of(wq2.getAsDouble() + 2 * wq * k.m1() + k.m2())
                    : OptionalDouble.empty();
            out.put(k.jobClass(), new Prediction(wq, wq2, wq + k.m1(), w2));
            sigmaBefore = sigma;
            r2Before = r2UpTo;
        }
        return out;
    }

    /** Probabilidade de espera de Erlang C para c servidores e carga oferecida a = λ·E[S]. */
    static double erlangC(int c, double a) {
        double b = 1;
        for (int k = 1; k <= c; k++) {
            b = a * b / (k + a * b);
        }
        double rho = a / c;
        return b / (1 - rho * (1 - b));
    }
}
