package br.com.filas.experimento.analysis;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Uso:
 * <pre>
 * analysis [opções] &lt;registro.jsonl&gt;...
 *   --warmup-s S        descarta os jobs que chegaram nos primeiros S segundos (padrão 0)
 *   --servers C         número de servidores (padrão: workers distintos no registro)
 *   --service B         measured (end − start, padrão) ou nominal (size_us) na teoria
 *   --model M           auto (pela política, padrão), fifo, priority ou none
 *   --replicas          agrega os arquivos como réplicas da mesma configuração
 *   --csv ARQ           grava uma linha por arquivo e classe
 * </pre>
 */
public final class AnalysisMain {

    private AnalysisMain() {
    }

    public static void main(String[] args) {
        try {
            // Java 17 escreve no console com a codificação da plataforma, que não tem λ nem ρ no Windows.
            System.exit(run(args, new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8)));
        } catch (IllegalArgumentException | IOException e) {
            System.err.println("[analysis] " + e.getMessage());
            System.exit(1);
        }
    }

    static int run(String[] args, PrintStream out) throws IOException {
        Duration warmup = Duration.ZERO;
        Optional<Integer> servers = Optional.empty();
        RunReport.ServiceBasis basis = RunReport.ServiceBasis.MEASURED;
        RunReport.ModelChoice model = RunReport.ModelChoice.AUTO;
        boolean replicas = false;
        Optional<Path> csv = Optional.empty();
        List<Path> files = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            switch (a) {
                case "--warmup-s" -> warmup = Duration.ofNanos(Math.round(Double.parseDouble(value(args, ++i, a)) * 1e9));
                case "--servers" -> servers = Optional.of(Integer.parseInt(value(args, ++i, a)));
                case "--service" -> basis = RunReport.ServiceBasis.valueOf(
                        value(args, ++i, a).toUpperCase(Locale.ROOT));
                case "--model" -> model = RunReport.ModelChoice.valueOf(value(args, ++i, a).toUpperCase(Locale.ROOT));
                case "--replicas" -> replicas = true;
                case "--csv" -> csv = Optional.of(Path.of(value(args, ++i, a)));
                default -> {
                    if (a.startsWith("--")) {
                        return usage("opção desconhecida: " + a);
                    }
                    files.add(Path.of(a));
                }
            }
        }
        if (files.isEmpty()) {
            return usage("nenhum registro informado");
        }

        List<RunReport> reports = new ArrayList<>();
        for (Path f : files) {
            RunReport r = RunReport.of(f, RunData.read(f, warmup), servers, basis, model);
            reports.add(r);
            out.println(r.render());
        }
        if (replicas) {
            out.println(Replicas.render(reports));
        }
        if (csv.isPresent()) {
            List<String> lines = new ArrayList<>();
            lines.add(RunReport.CSV_HEADER);
            reports.forEach(r -> lines.addAll(r.csvRows()));
            Path parent = csv.get().toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.write(csv.get(), lines, StandardCharsets.UTF_8);
            System.err.println("[analysis] resumo gravado em " + csv.get().toAbsolutePath());
        }
        return 0;
    }

    private static String value(String[] args, int i, String option) {
        if (i >= args.length) {
            throw new IllegalArgumentException("falta o valor de " + option);
        }
        return args[i];
    }

    private static int usage(String msg) {
        System.err.println("[analysis] " + msg);
        System.err.println("uso: analysis [--warmup-s S] [--servers C] [--service measured|nominal] "
                + "[--model auto|fifo|priority|none] [--replicas] [--csv ARQ] <registro.jsonl>...");
        return 2;
    }
}
