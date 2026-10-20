package br.com.filas.experimento.adapter.local;

import br.com.filas.experimento.domain.JobRecord;

import java.time.Duration;
import java.time.Instant;

/**
 * Serializa um {@link JobRecord} como uma linha JSON. Instantes em epoch e durações sempre em
 * microssegundos inteiros (sufixo {@code _us}): com serviço de 20 ms, a resolução de 1 ms
 * já seria 5% do valor.
 */
public final class JobRecordJson {

    private JobRecordJson() {
    }

    public static String toJson(JobRecord r) {
        StringBuilder sb = new StringBuilder(512).append('{');
        field(sb, "job_id", r.jobId().toString());
        field(sb, "run_id", r.runId());
        field(sb, "class", r.jobClass().name());
        field(sb, "size_us", micros(r.size()));
        field(sb, "enqueue_ts_us", micros(r.enqueueTs()));
        field(sb, "receive_ts_us", micros(r.receiveTs()));
        field(sb, "start_ts_us", micros(r.startTs()));
        field(sb, "end_ts_us", micros(r.endTs()));
        field(sb, "service_actual_us", micros(r.serviceActual()));
        field(sb, "worker_id", r.workerId());
        field(sb, "policy", r.policy().name());
        field(sb, "queue", r.queue().name());
        field(sb, "receive_count", r.receiveCount());
        field(sb, "parent_job_id", r.parentJobId().toString());
        field(sb, "slice_index", r.sliceIndex());
        field(sb, "slice_count", r.sliceCount());
        sb.setLength(sb.length() - 1);
        return sb.append('}').toString();
    }

    static long micros(Instant t) {
        return Math.addExact(Math.multiplyExact(t.getEpochSecond(), 1_000_000L), t.getNano() / 1_000);
    }

    static long micros(Duration d) {
        return Math.addExact(Math.multiplyExact(d.getSeconds(), 1_000_000L), d.getNano() / 1_000);
    }

    private static void field(StringBuilder sb, String name, long value) {
        sb.append('"').append(name).append("\":").append(value).append(',');
    }

    private static void field(StringBuilder sb, String name, String value) {
        sb.append('"').append(name).append("\":\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append("\",");
    }
}
