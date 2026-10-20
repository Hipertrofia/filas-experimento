package br.com.filas.experimento.generator;

import br.com.filas.experimento.domain.ClassSpec;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.TraceSpec;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Configuração de uma rodada do gerador. A carga vem de uma de duas formas:
 * <ul>
 *   <li>{@code load.rho}, {@code load.servers} e {@code load.mix-priority} (fração de jobs
 *       prioritários): as taxas saem de λ = ρ·c / E[S];</li>
 *   <li>{@code arrival.rate-per-s.priority} e {@code arrival.rate-per-s.normal}: taxas explícitas,
 *       necessárias quando E[S] = 0 (rodada de overhead).</li>
 * </ul>
 * Precedência: arquivo &lt; variável de ambiente ({@code load.rho} → {@code LOAD_RHO}) &lt; {@code -D}.
 */
public record GeneratorConfig(
        TraceSpec spec,
        Optional<Integer> servers,
        Path traceFile,
        Backend backend,
        Map<JobClass, QueueId> queueByClass,
        Optional<Duration> visibilityTimeout,
        Optional<URI> sqsEndpoint,
        String sqsRegion) {

    public enum Backend { LOCAL, SQS }

    public GeneratorConfig {
        queueByClass = Map.copyOf(queueByClass);
    }

    public static GeneratorConfig load(Path file, Map<String, String> env, Properties sysProps) throws IOException {
        Map<String, String> m = new HashMap<>();
        Properties p = new Properties();
        try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            p.load(r);
        }
        p.stringPropertyNames().forEach(k -> m.put(k, p.getProperty(k).trim()));
        for (String k : Keys.ALL) {
            String v = env.get(k.toUpperCase(Locale.ROOT).replace('.', '_').replace('-', '_'));
            if (v != null) {
                m.put(k, v.trim());
            }
            if (sysProps.getProperty(k) != null) {
                m.put(k, sysProps.getProperty(k).trim());
            }
        }
        return from(m);
    }

    static GeneratorConfig from(Map<String, String> m) {
        String runId = require(m, Keys.RUN_ID);
        long seed = Long.parseLong(require(m, Keys.SEED));
        Duration horizon = Duration.ofNanos(Math.round(Double.parseDouble(require(m, Keys.HORIZON_S)) * 1e9));
        Duration meanP = millis(m.getOrDefault(Keys.MEAN_PRIORITY, "20"));
        Duration meanN = millis(m.getOrDefault(Keys.MEAN_NORMAL, "200"));
        int slicesP = Integer.parseInt(m.getOrDefault(Keys.SLICES_PRIORITY, "1"));
        int slicesN = Integer.parseInt(m.getOrDefault(Keys.SLICES_NORMAL, "1"));
        Optional<Integer> servers = Optional.ofNullable(m.get(Keys.SERVERS)).map(Integer::parseInt);

        boolean explicit = m.containsKey(Keys.RATE_PRIORITY) || m.containsKey(Keys.RATE_NORMAL);
        boolean byLoad = m.containsKey(Keys.RHO) || m.containsKey(Keys.MIX);
        TraceSpec spec;
        if (explicit && byLoad) {
            throw new IllegalArgumentException("use taxas explícitas (arrival.rate-per-s.*) ou carga (load.rho, "
                    + "load.mix-priority), não os dois");
        } else if (explicit) {
            Map<JobClass, ClassSpec> classes = new EnumMap<>(JobClass.class);
            classes.put(JobClass.PRIORITY, new ClassSpec(
                    Double.parseDouble(m.getOrDefault(Keys.RATE_PRIORITY, "0")), meanP, slicesP));
            classes.put(JobClass.NORMAL, new ClassSpec(
                    Double.parseDouble(m.getOrDefault(Keys.RATE_NORMAL, "0")), meanN, slicesN));
            spec = new TraceSpec(runId, seed, horizon, classes);
        } else {
            spec = TraceSpec.fromUtilization(runId, seed, horizon,
                    Double.parseDouble(require(m, Keys.RHO)),
                    servers.orElseThrow(() -> new IllegalArgumentException("configuração obrigatória ausente: "
                            + Keys.SERVERS)),
                    Double.parseDouble(require(m, Keys.MIX)),
                    meanP, meanN, slicesP, slicesN);
        }

        Map<JobClass, QueueId> queues = new EnumMap<>(JobClass.class);
        queues.put(JobClass.PRIORITY, new QueueId(m.getOrDefault(Keys.QUEUE_PRIORITY, "prioritaria")));
        queues.put(JobClass.NORMAL, new QueueId(m.getOrDefault(Keys.QUEUE_NORMAL, "normal")));
        return new GeneratorConfig(
                spec,
                servers,
                Path.of(m.getOrDefault(Keys.TRACE_FILE, "experiments/traces/" + runId + ".csv")),
                Backend.valueOf(m.getOrDefault(Keys.BACKEND, "SQS").toUpperCase(Locale.ROOT)),
                queues,
                Optional.ofNullable(m.get(Keys.VISIBILITY_MS)).map(v -> Duration.ofMillis(Long.parseLong(v.trim()))),
                Optional.ofNullable(m.get(Keys.SQS_ENDPOINT)).filter(v -> !v.isBlank()).map(URI::create),
                m.getOrDefault(Keys.SQS_REGION, "us-east-1"));
    }

    private static String require(Map<String, String> m, String key) {
        String v = m.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("configuração obrigatória ausente: " + key);
        }
        return v;
    }

    private static Duration millis(String v) {
        return Duration.ofNanos(Math.round(Double.parseDouble(v.trim()) * 1e6));
    }

    static final class Keys {
        static final String RUN_ID = "run.id";
        static final String SEED = "seed";
        static final String HORIZON_S = "horizon-s";
        static final String RHO = "load.rho";
        static final String SERVERS = "load.servers";
        static final String MIX = "load.mix-priority";
        static final String RATE_PRIORITY = "arrival.rate-per-s.priority";
        static final String RATE_NORMAL = "arrival.rate-per-s.normal";
        static final String MEAN_PRIORITY = "size.mean-ms.priority";
        static final String MEAN_NORMAL = "size.mean-ms.normal";
        static final String SLICES_PRIORITY = "slice.count.priority";
        static final String SLICES_NORMAL = "slice.count.normal";
        static final String TRACE_FILE = "trace.file";
        static final String BACKEND = "queue.backend";
        static final String QUEUE_PRIORITY = "queue.priority";
        static final String QUEUE_NORMAL = "queue.normal";
        static final String VISIBILITY_MS = "queue.visibility-timeout-ms";
        static final String SQS_ENDPOINT = "sqs.endpoint";
        static final String SQS_REGION = "sqs.region";

        static final List<String> ALL = List.of(RUN_ID, SEED, HORIZON_S, RHO, SERVERS, MIX, RATE_PRIORITY,
                RATE_NORMAL, MEAN_PRIORITY, MEAN_NORMAL, SLICES_PRIORITY, SLICES_NORMAL, TRACE_FILE, BACKEND,
                QUEUE_PRIORITY, QUEUE_NORMAL, VISIBILITY_MS, SQS_ENDPOINT, SQS_REGION);

        private Keys() {
        }
    }
}
