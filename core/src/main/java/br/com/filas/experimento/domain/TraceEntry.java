package br.com.filas.experimento.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Uma chegada do trace.
 *
 * @param offset deslocamento da chegada desde o início do replay
 * @param size   serviço total do job; com {@code sliceCount = m}, cada fatia tem {@code size / m}
 */
public record TraceEntry(Duration offset, UUID jobId, JobClass jobClass, Duration size, int sliceCount) {

    public TraceEntry {
        Objects.requireNonNull(offset, "offset");
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(jobClass, "jobClass");
        Objects.requireNonNull(size, "size");
        if (offset.isNegative()) {
            throw new IllegalArgumentException("offset negativo: " + offset);
        }
        if (size.isNegative()) {
            throw new IllegalArgumentException("size negativo: " + size);
        }
        if (sliceCount < 1) {
            throw new IllegalArgumentException("sliceCount < 1: " + sliceCount);
        }
    }

    public Duration sliceSize() {
        return Duration.ofNanos(size.toNanos() / sliceCount);
    }

    /** Mensagem a enviar na chegada: a primeira fatia (o job inteiro, se {@code sliceCount = 1}). */
    public Job firstSlice(String runId, Instant enqueueTs) {
        return new Job(jobId, runId, jobClass, sliceSize(), enqueueTs, jobId, 0, sliceCount);
    }
}
