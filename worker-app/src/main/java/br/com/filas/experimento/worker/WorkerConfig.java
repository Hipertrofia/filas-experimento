package br.com.filas.experimento.worker;

import br.com.filas.experimento.domain.ConsumptionPolicy;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.QueueId;

import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Configuração do worker. Precedência: padrões &lt; arquivo .properties (1º argumento) &lt;
 * variáveis de ambiente ({@code worker.policy} → {@code WORKER_POLICY}) &lt; {@code -D}.
 */
public record WorkerConfig(
        String workerId,
        int threads,
        ConsumptionPolicy policy,
        List<QueueId> queues,
        ServiceMode serviceMode,
        Backend backend,
        Duration defaultWait,
        Map<QueueId, Duration> waitTimes,
        Duration visibilityTimeout,
        Path sinkFile,
        Duration flushInterval,
        Duration shutdownTimeout,
        Optional<Path> localTrace,
        Optional<Duration> localSpinMargin,
        Map<JobClass, QueueId> localQueueByClass,
        boolean exitWhenDrained,
        Optional<URI> sqsEndpoint,
        String sqsRegion,
        Duration idlePause,
        Duration exitAfterIdle) {

    public enum ServiceMode { SLEEP, CPU_BURN }

    public enum Backend { LOCAL, SQS }

    static final String WAIT_PREFIX = "queue.wait-ms.";

    public WorkerConfig {
        queues = List.copyOf(queues);
        waitTimes = Map.copyOf(waitTimes);
        localQueueByClass = Map.copyOf(localQueueByClass);
        if (threads < 1) {
            throw new IllegalArgumentException("worker.threads < 1: " + threads);
        }
        if (policy == ConsumptionPolicy.FIFO_COMPARTILHADO && queues.size() != 1) {
            throw new IllegalArgumentException("FIFO_COMPARTILHADO usa exatamente uma fila: " + queues);
        }
        if (policy == ConsumptionPolicy.PRIORIDADE && queues.size() < 2) {
            throw new IllegalArgumentException("PRIORIDADE precisa de prioritária e normal: " + queues);
        }
        for (QueueId q : waitTimes.keySet()) {
            if (!queues.contains(q)) {
                throw new IllegalArgumentException(WAIT_PREFIX + q + " não está em worker.queues");
            }
        }
        if (backend == Backend.SQS && (localTrace.isPresent() || exitWhenDrained)) {
            throw new IllegalArgumentException("local.trace e local.exit-when-drained só valem com queue.backend=LOCAL; "
                    + "no SQS as chegadas vêm do generator-app");
        }
    }

    /** Ids dos workers deste processo: {@code <worker.id>/t<i>}. */
    public List<String> threadIds() {
        return java.util.stream.IntStream.range(0, threads).mapToObj(i -> workerId + "/t" + i).toList();
    }

    public static WorkerConfig load(String[] args, Map<String, String> env, Properties sysProps)
            throws IOException {
        Map<String, String> file = new HashMap<>();
        if (args.length > 0) {
            Properties p = new Properties();
            try (Reader r = Files.newBufferedReader(Path.of(args[0]), StandardCharsets.UTF_8)) {
                p.load(r);
            }
            p.stringPropertyNames().forEach(k -> file.put(k, p.getProperty(k).trim()));
        }
        return from(merge(file, env, sysProps));
    }

    static Map<String, String> merge(Map<String, String> file, Map<String, String> env, Properties sysProps) {
        Map<String, String> merged = new LinkedHashMap<>(file);
        // As chaves por fila (queue.wait-ms.<nome>) só vêm do arquivo ou de -D: o nome da fila
        // não sobrevive à conversão para variável de ambiente.
        for (String key : Keys.ALL) {
            String v = env.get(envName(key));
            if (v != null) {
                merged.put(key, v.trim());
            }
        }
        sysProps.stringPropertyNames().stream()
                .filter(k -> Keys.ALL.contains(k) || k.startsWith(WAIT_PREFIX))
                .forEach(k -> merged.put(k, sysProps.getProperty(k).trim()));
        return merged;
    }

    static String envName(String key) {
        return key.toUpperCase(Locale.ROOT).replace('.', '_').replace('-', '_');
    }

    static WorkerConfig from(Map<String, String> m) {
        List<QueueId> queues = Arrays.stream(require(m, Keys.QUEUES).split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).map(QueueId::new).toList();
        Map<QueueId, Duration> waits = new HashMap<>();
        m.forEach((k, v) -> {
            if (k.startsWith(WAIT_PREFIX)) {
                waits.put(new QueueId(k.substring(WAIT_PREFIX.length())), millis(k, v));
            }
        });
        String workerId = m.getOrDefault(Keys.ID, defaultWorkerId());
        Map<JobClass, QueueId> byClass = new HashMap<>();
        if (!queues.isEmpty()) {
            QueueId prio = queues.get(0);
            QueueId normal = queues.size() > 1 ? queues.get(1) : prio;
            byClass.put(JobClass.PRIORITY, new QueueId(m.getOrDefault(Keys.LOCAL_QUEUE_PRIORITY, prio.name())));
            byClass.put(JobClass.NORMAL, new QueueId(m.getOrDefault(Keys.LOCAL_QUEUE_NORMAL, normal.name())));
        }
        return new WorkerConfig(
                workerId,
                Integer.parseInt(m.getOrDefault(Keys.THREADS, "1")),
                ConsumptionPolicy.valueOf(require(m, Keys.POLICY).toUpperCase(Locale.ROOT)),
                queues,
                ServiceMode.valueOf(m.getOrDefault(Keys.SERVICE_MODE, "SLEEP").toUpperCase(Locale.ROOT)),
                Backend.valueOf(m.getOrDefault(Keys.BACKEND, "LOCAL").toUpperCase(Locale.ROOT)),
                millis(Keys.WAIT_MS, m.getOrDefault(Keys.WAIT_MS, "20")),
                waits,
                millis(Keys.VISIBILITY_MS, m.getOrDefault(Keys.VISIBILITY_MS, "30000")),
                Path.of(m.getOrDefault(Keys.SINK_FILE, "registros/" + safeFileName(workerId) + ".jsonl")),
                millis(Keys.SINK_FLUSH_MS, m.getOrDefault(Keys.SINK_FLUSH_MS, "500")),
                millis(Keys.SHUTDOWN_MS, m.getOrDefault(Keys.SHUTDOWN_MS, "30000")),
                Optional.ofNullable(m.get(Keys.LOCAL_TRACE)).filter(s -> !s.isBlank()).map(Path::of),
                Optional.ofNullable(m.get(Keys.LOCAL_SPIN_MS)).map(v -> millis(Keys.LOCAL_SPIN_MS, v)),
                byClass,
                Boolean.parseBoolean(m.getOrDefault(Keys.LOCAL_EXIT_WHEN_DRAINED, "false")),
                Optional.ofNullable(m.get(Keys.SQS_ENDPOINT)).filter(s -> !s.isBlank()).map(URI::create),
                m.getOrDefault(Keys.SQS_REGION, "us-east-1"),
                millis(Keys.IDLE_PAUSE_MS, m.getOrDefault(Keys.IDLE_PAUSE_MS, "0")),
                millis(Keys.EXIT_AFTER_IDLE_MS, m.getOrDefault(Keys.EXIT_AFTER_IDLE_MS, "0")));
    }

    private static String require(Map<String, String> m, String key) {
        String v = m.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("configuração obrigatória ausente: " + key
                    + " (ou " + envName(key) + ")");
        }
        return v;
    }

    private static Duration millis(String key, String v) {
        long ms = Long.parseLong(v.trim());
        if (ms < 0) {
            throw new IllegalArgumentException(key + " negativo: " + ms);
        }
        return Duration.ofMillis(ms);
    }

    private static String defaultWorkerId() {
        String host = System.getenv("HOSTNAME");
        if (host != null && !host.isBlank()) {
            return host;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "worker";
        }
    }

    static String safeFileName(String s) {
        return s.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    static final class Keys {
        static final String ID = "worker.id";
        static final String THREADS = "worker.threads";
        static final String POLICY = "worker.policy";
        static final String QUEUES = "worker.queues";
        static final String SERVICE_MODE = "service.mode";
        static final String BACKEND = "queue.backend";
        static final String WAIT_MS = "queue.wait-ms";
        static final String VISIBILITY_MS = "queue.visibility-timeout-ms";
        static final String SINK_FILE = "sink.file";
        static final String SINK_FLUSH_MS = "sink.flush-ms";
        static final String SHUTDOWN_MS = "shutdown.timeout-ms";
        static final String LOCAL_TRACE = "local.trace";
        static final String LOCAL_SPIN_MS = "local.spin-ms";
        static final String LOCAL_QUEUE_PRIORITY = "local.queue.priority";
        static final String LOCAL_QUEUE_NORMAL = "local.queue.normal";
        static final String LOCAL_EXIT_WHEN_DRAINED = "local.exit-when-drained";

        static final String SQS_ENDPOINT = "sqs.endpoint";
        static final String SQS_REGION = "sqs.region";
        static final String IDLE_PAUSE_MS = "worker.idle-pause-ms";
        static final String EXIT_AFTER_IDLE_MS = "worker.exit-after-idle-ms";

        static final List<String> ALL = List.of(ID, THREADS, POLICY, QUEUES, SERVICE_MODE, BACKEND,
                WAIT_MS, VISIBILITY_MS, SINK_FILE, SINK_FLUSH_MS, SHUTDOWN_MS, LOCAL_TRACE, LOCAL_SPIN_MS,
                LOCAL_QUEUE_PRIORITY, LOCAL_QUEUE_NORMAL, LOCAL_EXIT_WHEN_DRAINED, SQS_ENDPOINT, SQS_REGION,
                IDLE_PAUSE_MS, EXIT_AFTER_IDLE_MS);

        private Keys() {
        }
    }
}
