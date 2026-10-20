package br.com.filas.experimento.application;

import br.com.filas.experimento.domain.ClassSpec;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.Trace;
import br.com.filas.experimento.domain.TraceEntry;
import br.com.filas.experimento.domain.TraceSpec;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.UUID;

/**
 * Gera o trace de chegadas. Cada classe tem dois fluxos aleatórios próprios (chegadas e
 * tamanhos), derivados da seed numa ordem fixa. Assim, com a mesma seed, mudar a taxa só
 * reescala os intervalos, e mudar uma classe não altera a outra (números aleatórios comuns
 * entre rodadas).
 *
 * <p>Offsets e tamanhos são arredondados para microssegundos, a resolução do arquivo.
 */
public final class PoissonTraceGenerator {

    private PoissonTraceGenerator() {
    }

    public static Trace generate(TraceSpec spec) {
        SplittableRandom root = new SplittableRandom(spec.seed());
        double horizonSeconds = spec.horizon().toNanos() / 1e9;
        List<TraceEntry> entries = new ArrayList<>();
        for (JobClass jobClass : JobClass.values()) {
            SplittableRandom arrivals = root.split();
            SplittableRandom sizes = root.split();
            ClassSpec cs = spec.classes().get(jobClass);
            if (cs == null || cs.ratePerSecond() == 0) {
                continue;
            }
            double meanSizeMicros = cs.meanSize().toNanos() / 1e3;
            double t = 0;
            for (int i = 0; ; i++) {
                t += exponential(arrivals, 1 / cs.ratePerSecond());
                long offsetMicros = Math.round(t * 1e6);
                if (offsetMicros >= Math.round(horizonSeconds * 1e6)) {
                    break;
                }
                long sizeMicros = Math.round(exponential(sizes, meanSizeMicros));
                entries.add(new TraceEntry(
                        Duration.ofNanos(offsetMicros * 1_000),
                        jobId(spec, jobClass, i),
                        jobClass,
                        Duration.ofNanos(sizeMicros * 1_000),
                        cs.sliceCount()));
            }
        }
        // Ordenação estável: em empate de offset, a ordem das classes decide.
        entries.sort(Comparator.comparing(TraceEntry::offset));
        return new Trace(spec.runId(), spec.seed(), metadata(spec), entries);
    }

    private static double exponential(SplittableRandom rnd, double mean) {
        return -mean * Math.log(1 - rnd.nextDouble());
    }

    private static UUID jobId(TraceSpec spec, JobClass jobClass, int index) {
        return UUID.nameUUIDFromBytes(
                (spec.runId() + ":" + spec.seed() + ":" + jobClass + ":" + index).getBytes(StandardCharsets.UTF_8));
    }

    private static Map<String, String> metadata(TraceSpec spec) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("horizon_s", fmt(spec.horizon().toNanos() / 1e9));
        for (JobClass c : JobClass.values()) {
            ClassSpec cs = spec.classes().get(c);
            if (cs != null) {
                String k = c.name().toLowerCase(Locale.ROOT);
                m.put(k + ".rate_per_s", fmt(cs.ratePerSecond()));
                m.put(k + ".mean_size_ms", fmt(cs.meanSize().toNanos() / 1e6));
                m.put(k + ".slice_count", String.valueOf(cs.sliceCount()));
            }
        }
        m.put("offered_load_servers", fmt(spec.offeredLoad()));
        return m;
    }

    private static String fmt(double v) {
        return String.format(Locale.ROOT, "%.6g", v);
    }
}
