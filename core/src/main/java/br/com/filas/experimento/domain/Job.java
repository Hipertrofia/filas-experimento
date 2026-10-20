package br.com.filas.experimento.domain;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Job como chega na mensagem. O tamanho já vem sorteado pelo gerador; o worker não tem
 * aleatoriedade própria.
 *
 * @param size      tempo de serviço nominal (para uma fatia, já é {@code size_ms / slice_count})
 * @param enqueueTs instante de envio, relógio de parede
 */
public record Job(
        UUID jobId,
        String runId,
        JobClass jobClass,
        Duration size,
        Instant enqueueTs,
        UUID parentJobId,
        int sliceIndex,
        int sliceCount) {

    public Job {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(jobClass, "jobClass");
        Objects.requireNonNull(size, "size");
        Objects.requireNonNull(enqueueTs, "enqueueTs");
        Objects.requireNonNull(parentJobId, "parentJobId");
        if (size.isNegative()) {
            throw new IllegalArgumentException("size negativo: " + size);
        }
        if (sliceCount < 1) {
            throw new IllegalArgumentException("sliceCount < 1: " + sliceCount);
        }
        if (sliceIndex < 0 || sliceIndex >= sliceCount) {
            throw new IllegalArgumentException(
                    "sliceIndex fora de [0, " + sliceCount + "): " + sliceIndex);
        }
    }

    /** Job sem fatiamento: {@code parent_job_id = job_id}, fatia 0 de 1. */
    public static Job whole(UUID jobId, String runId, JobClass jobClass, Duration size, Instant enqueueTs) {
        return new Job(jobId, runId, jobClass, size, enqueueTs, jobId, 0, 1);
    }

    public boolean hasNextSlice() {
        return sliceIndex + 1 < sliceCount;
    }

    /**
     * Próxima fatia, para reenfileirar no fim da fila da classe. O id é derivado de
     * {@code parent_job_id} e do índice, então é o mesmo em qualquer reexecução do trace.
     */
    public Job nextSlice(Instant enqueueTs) {
        if (!hasNextSlice()) {
            throw new IllegalStateException("última fatia: " + sliceIndex + "/" + sliceCount);
        }
        int next = sliceIndex + 1;
        UUID nextId = UUID.nameUUIDFromBytes(
                (parentJobId + ":" + next).getBytes(StandardCharsets.UTF_8));
        return new Job(nextId, runId, jobClass, size, enqueueTs, parentJobId, next, sliceCount);
    }
}
