package br.com.filas.experimento.adapter.local;

import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.Trace;
import br.com.filas.experimento.domain.TraceEntry;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Arquivo de trace. Cabeçalho com {@code # chave=valor} ({@code run_id} e {@code seed}
 * obrigatórios, o resto informativo), depois a linha de colunas e uma chegada por linha:
 *
 * <pre>
 * # run_id=rho080-s1
 * # seed=1
 * offset_us,job_id,class,size_us,slice_count
 * 1234,6f1c...,NORMAL,187345,1
 * </pre>
 *
 * {@code size_us} é o serviço total do job; cada fatia recebe {@code size_us / slice_count}.
 */
public final class TraceCsv {

    static final String COLUMNS = "offset_us,job_id,class,size_us,slice_count";

    private TraceCsv() {
    }

    public static void write(Trace trace, Path file) throws IOException {
        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            w.write("# run_id=" + trace.runId() + "\n");
            w.write("# seed=" + trace.seed() + "\n");
            for (Map.Entry<String, String> m : trace.metadata().entrySet()) {
                w.write("# " + m.getKey() + "=" + m.getValue() + "\n");
            }
            w.write(COLUMNS + "\n");
            StringBuilder sb = new StringBuilder(96);
            for (TraceEntry e : trace.entries()) {
                sb.setLength(0);
                sb.append(JobRecordJson.micros(e.offset())).append(',')
                  .append(e.jobId()).append(',')
                  .append(e.jobClass().name()).append(',')
                  .append(JobRecordJson.micros(e.size())).append(',')
                  .append(e.sliceCount()).append('\n');
                w.write(sb.toString());
            }
        }
    }

    public static Trace read(Path file) throws IOException {
        Map<String, String> header = new LinkedHashMap<>();
        List<TraceEntry> entries = new ArrayList<>();
        boolean columns = false;
        int lineNo = 0;
        for (String raw : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            lineNo++;
            String line = raw.trim();
            if (line.isEmpty()) {
                continue;
            }
            try {
                if (line.startsWith("#")) {
                    int eq = line.indexOf('=');
                    if (eq > 0) {
                        header.put(line.substring(1, eq).trim(), line.substring(eq + 1).trim());
                    }
                } else if (!columns) {
                    if (!line.equals(COLUMNS)) {
                        throw new IllegalArgumentException("esperado cabeçalho " + COLUMNS);
                    }
                    columns = true;
                } else {
                    entries.add(parse(line));
                }
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(file + ":" + lineNo + ": " + e.getMessage(), e);
            }
        }
        String runId = header.remove("run_id");
        String seed = header.remove("seed");
        if (runId == null || seed == null) {
            throw new IllegalArgumentException(file + ": cabeçalho sem run_id ou seed");
        }
        return new Trace(runId, Long.parseLong(seed), header, entries);
    }

    private static TraceEntry parse(String line) {
        String[] c = line.split(",");
        if (c.length != 5) {
            throw new IllegalArgumentException("esperadas 5 colunas: " + line);
        }
        return new TraceEntry(
                Duration.ofNanos(Long.parseLong(c[0].trim()) * 1_000),
                UUID.fromString(c[1].trim()),
                JobClass.valueOf(c[2].trim()),
                Duration.ofNanos(Long.parseLong(c[3].trim()) * 1_000),
                Integer.parseInt(c[4].trim()));
    }
}
