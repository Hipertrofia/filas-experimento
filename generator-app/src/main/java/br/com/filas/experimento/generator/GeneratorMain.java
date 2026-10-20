package br.com.filas.experimento.generator;

import br.com.filas.experimento.adapter.local.PreciseSleeper;
import br.com.filas.experimento.adapter.local.SystemClockAdapter;
import br.com.filas.experimento.adapter.local.TraceCsv;
import br.com.filas.experimento.adapter.sqs.SqsJobQueue;
import br.com.filas.experimento.application.PoissonTraceGenerator;
import br.com.filas.experimento.application.TraceReplayer;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.Trace;
import br.com.filas.experimento.domain.TraceEntry;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Uso:
 * <pre>
 * generator-app generate &lt;rodada.properties&gt;   sorteia o trace e grava em trace.file
 * generator-app send     &lt;rodada.properties&gt;   envia trace.file para a fila, em tempo real
 * </pre>
 */
public final class GeneratorMain {

    /** Atraso de envio acima do qual as chegadas já não podem ser tratadas como Poisson. */
    static final Duration LAG_WARNING = Duration.ofMillis(5);

    private GeneratorMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !(args[0].equals("generate") || args[0].equals("send"))) {
            System.err.println("uso: generator-app (generate|send) <rodada.properties>");
            System.exit(2);
        }
        GeneratorConfig cfg = GeneratorConfig.load(Path.of(args[1]), System.getenv(), System.getProperties());
        if (args[0].equals("generate")) {
            generate(cfg);
        } else {
            sendWithShutdownHook(cfg);
        }
    }

    static Trace generate(GeneratorConfig cfg) throws Exception {
        Trace trace = PoissonTraceGenerator.generate(cfg.spec());
        TraceCsv.write(trace, cfg.traceFile());
        log("trace " + trace.runId() + " (seed " + trace.seed() + "): " + trace.entries().size()
                + " chegadas em " + cfg.traceFile().toAbsolutePath());
        log(String.format(Locale.ROOT, "parâmetros: λ_prio=%.4f/s  λ_normal=%.4f/s",
                cfg.spec().classes().get(JobClass.PRIORITY).ratePerSecond(),
                cfg.spec().classes().get(JobClass.NORMAL).ratePerSecond()));
        log("medido no trace:\n" + TraceSummary.of(trace, cfg.spec().horizon()).render(cfg.servers()));
        return trace;
    }

    private static void sendWithShutdownHook(GeneratorConfig cfg) throws Exception {
        Thread main = Thread.currentThread();
        CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (done.getCount() > 0) {
                log("sinal de parada: interrompendo o envio");
                main.interrupt();
                try {
                    done.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "shutdown"));
        try {
            send(cfg);
        } finally {
            done.countDown();
        }
    }

    static TraceReplayer.ReplayStats send(GeneratorConfig cfg) throws Exception {
        Trace trace = TraceCsv.read(cfg.traceFile());
        if (cfg.backend() == GeneratorConfig.Backend.LOCAL) {
            throw new UnsupportedOperationException("a fila local vive no processo do worker: "
                    + "use local.trace no worker-app para fazer o replay de " + cfg.traceFile());
        }
        if (cfg.visibilityTimeout().isPresent()) {
            for (TraceEntry e : trace.entries()) {
                if (e.sliceSize().compareTo(cfg.visibilityTimeout().get()) >= 0) {
                    throw new IllegalArgumentException("job " + e.jobId() + " com serviço " + e.sliceSize()
                            + " não cabe no visibility timeout " + cfg.visibilityTimeout().get());
                }
            }
        }
        // O gerador só envia: a espera e o visibility timeout do receive não se aplicam.
        SqsJobQueue.Settings settings = new SqsJobQueue.Settings(Map.of(), Duration.ZERO, Duration.ofSeconds(30));
        try (SqsJobQueue sqs = new SqsJobQueue(SqsJobQueue.client(cfg.sqsEndpoint(), cfg.sqsRegion()), settings)) {
            sqs.resolve(List.copyOf(new LinkedHashSet<>(cfg.queueByClass().values())));
            TraceReplayer replayer = new TraceReplayer(
                    sqs, new SystemClockAdapter(), PreciseSleeper.forPlatform(), cfg.queueByClass());
            log("enviando " + trace.entries().size() + " chegadas de " + trace.runId() + " para " + cfg.queueByClass());
            TraceReplayer.ReplayStats s;
            try {
                s = replayer.replay(trace);
            } catch (InterruptedException e) {
                log("envio interrompido");
                throw e;
            }
            log(String.format(Locale.ROOT, "%d/%d enviadas; atraso médio %.3f ms, máximo %.3f ms",
                    s.sent(), s.total(), s.meanLag().toNanos() / 1e6, s.maxLag().toNanos() / 1e6));
            if (s.maxLag().compareTo(LAG_WARNING) > 0) {
                log("ATENÇÃO: atraso máximo acima de " + LAG_WARNING.toMillis()
                        + " ms; as chegadas podem ter deixado de ser Poisson");
            }
            return s;
        }
    }

    private static void log(String msg) {
        System.err.println("[generator] " + msg);
    }
}
