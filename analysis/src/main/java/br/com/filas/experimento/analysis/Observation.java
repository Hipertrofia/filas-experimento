package br.com.filas.experimento.analysis;

import br.com.filas.experimento.domain.ConsumptionPolicy;
import br.com.filas.experimento.domain.JobClass;

import java.util.Map;
import java.util.UUID;

/** Uma linha do registro do worker (um atendimento), com instantes e durações em µs. */
record Observation(
        UUID jobId,
        String runId,
        JobClass jobClass,
        long sizeUs,
        long enqueueUs,
        long receiveUs,
        long startUs,
        long endUs,
        long serviceActualUs,
        String workerId,
        ConsumptionPolicy policy,
        String queue,
        int receiveCount,
        UUID parentJobId,
        int sliceIndex,
        int sliceCount) {

    static Observation parse(String line) {
        Map<String, String> m = JsonLine.parse(line);
        return new Observation(
                UUID.fromString(text(m, "job_id")),
                text(m, "run_id"),
                JobClass.valueOf(text(m, "class")),
                number(m, "size_us"),
                number(m, "enqueue_ts_us"),
                number(m, "receive_ts_us"),
                number(m, "start_ts_us"),
                number(m, "end_ts_us"),
                number(m, "service_actual_us"),
                text(m, "worker_id"),
                ConsumptionPolicy.valueOf(text(m, "policy")),
                text(m, "queue"),
                Math.toIntExact(number(m, "receive_count")),
                UUID.fromString(text(m, "parent_job_id")),
                Math.toIntExact(number(m, "slice_index")),
                Math.toIntExact(number(m, "slice_count")));
    }

    private static String text(Map<String, String> m, String key) {
        String v = m.get(key);
        if (v == null) {
            throw new IllegalArgumentException("campo ausente: " + key);
        }
        return v;
    }

    private static long number(Map<String, String> m, String key) {
        return Long.parseLong(text(m, key));
    }
}
