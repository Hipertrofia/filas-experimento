package br.com.filas.experimento.analysis;

import br.com.filas.experimento.domain.JobClass;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.ToDoubleFunction;

/**
 * Agregado de réplicas independentes (seeds diferentes): média das estimativas de cada rodada e
 * intervalo de 95% pela t de Student. Dentro de uma rodada as amostras são autocorrelacionadas,
 * por isso o intervalo vem só da variação entre rodadas.
 */
final class Replicas {

    private static final double[] T975 = {
            12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228,
            2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086,
            2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042};

    record Estimate(int n, double mean, double halfWidth) { }

    private Replicas() {
    }

    static Estimate estimate(double[] xs) {
        int n = xs.length;
        double mean = 0;
        for (double x : xs) {
            mean += x;
        }
        mean /= n;
        if (n < 2) {
            return new Estimate(n, mean, Double.NaN);
        }
        double ss = 0;
        for (double x : xs) {
            ss += (x - mean) * (x - mean);
        }
        double sd = Math.sqrt(ss / (n - 1));
        return new Estimate(n, mean, t975(n - 1) * sd / Math.sqrt(n));
    }

    static double t975(int df) {
        return df <= T975.length ? T975[df - 1] : 1.96;
    }

    static String render(List<RunReport> runs) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(Locale.ROOT, "== réplicas: %d rodadas (média ± meia-largura do IC 95%%, ms)%n", runs.size()));
        sb.append(String.format(Locale.ROOT, "%-8s %-22s %-22s %-22s %-22s %10s %10s%n",
                "classe", "E[Wq]", "E[Wq²]", "p99 Wq", "E[W]", "teoria Wq", "teoria W"));
        for (JobClass k : JobClass.values()) {
            List<RunReport> with = runs.stream().filter(r -> r.classes().containsKey(k)).toList();
            if (with.isEmpty()) {
                continue;
            }
            sb.append(String.format(Locale.ROOT, "%-8s %-22s %-22s %-22s %-22s %10s %10s%n", k,
                    fmt(with, k, c -> c.wq().mean(), "%.2f"),
                    fmt(with, k, c -> c.wq().secondMoment(), "%.0f"),
                    fmt(with, k, c -> c.wq().p99(), "%.1f"),
                    fmt(with, k, c -> c.w().mean(), "%.2f"),
                    theory(with, k, p -> p.wq()),
                    theory(with, k, p -> p.w())));
        }
        return sb.toString();
    }

    private static String fmt(List<RunReport> runs, JobClass k, ToDoubleFunction<RunReport.ClassResult> f,
                              String numFmt) {
        Estimate e = estimate(runs.stream().mapToDouble(r -> f.applyAsDouble(r.classes().get(k))).toArray());
        String m = String.format(Locale.ROOT, numFmt, e.mean());
        return Double.isNaN(e.halfWidth()) ? m : m + " ± " + String.format(Locale.ROOT, numFmt, e.halfWidth());
    }

    /** Média das previsões de cada rodada (cada uma usa os parâmetros medidos nela). */
    private static String theory(List<RunReport> runs, JobClass k, ToDoubleFunction<Theory.Prediction> f) {
        double[] xs = runs.stream()
                .map(r -> r.theory().get(k))
                .filter(Objects::nonNull)
                .mapToDouble(f)
                .toArray();
        return xs.length == runs.size() ? String.format(Locale.ROOT, "%.2f", estimate(xs).mean()) : "—";
    }
}
