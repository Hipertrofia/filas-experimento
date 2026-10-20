package br.com.filas.experimento.generator;

import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.Trace;
import br.com.filas.experimento.domain.TraceEntry;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/** Estatísticas empíricas do trace, para conferir contra os parâmetros antes de rodar. */
public record TraceSummary(Duration horizon, List<ClassSummary> classes) {

    public record ClassSummary(JobClass jobClass, int count, double ratePerSecond,
                               double meanSizeMs, double secondMomentMs2, double maxSizeMs) { }

    public static TraceSummary of(Trace trace, Duration horizon) {
        double seconds = horizon.toNanos() / 1e9;
        List<ClassSummary> classes = java.util.Arrays.stream(JobClass.values()).map(c -> {
            List<TraceEntry> es = trace.entries().stream().filter(e -> e.jobClass() == c).toList();
            double[] ms = es.stream().mapToDouble(e -> e.size().toNanos() / 1e6).toArray();
            double mean = java.util.Arrays.stream(ms).average().orElse(0);
            double m2 = java.util.Arrays.stream(ms).map(x -> x * x).average().orElse(0);
            double max = java.util.Arrays.stream(ms).max().orElse(0);
            return new ClassSummary(c, es.size(), es.size() / seconds, mean, m2, max);
        }).toList();
        return new TraceSummary(horizon, classes);
    }

    /** Carga oferecida medida no trace, em servidores. */
    public double offeredLoad() {
        return classes.stream().mapToDouble(c -> c.ratePerSecond() * c.meanSizeMs() / 1e3).sum();
    }

    public String render(Optional<Integer> servers) {
        StringBuilder sb = new StringBuilder();
        for (ClassSummary c : classes) {
            sb.append(String.format(Locale.ROOT,
                    "  %-8s n=%-7d λ=%.3f/s  E[S]=%.3f ms  E[S²]=%.1f ms²  max=%.1f ms%n",
                    c.jobClass(), c.count(), c.ratePerSecond(), c.meanSizeMs(), c.secondMomentMs2(), c.maxSizeMs()));
        }
        sb.append(String.format(Locale.ROOT, "  carga oferecida=%.4f servidores", offeredLoad()));
        servers.ifPresent(s -> sb.append(String.format(Locale.ROOT, "  ρ=%.4f (c=%d)", offeredLoad() / s, s)));
        return sb.toString();
    }
}
