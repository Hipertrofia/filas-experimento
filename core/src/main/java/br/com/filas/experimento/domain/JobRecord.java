package br.com.filas.experimento.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Registro de um atendimento. Os instantes são de parede (comparáveis entre pods);
 * {@code serviceActual} vem do relógio monotônico. Wq, W e serviço são derivados na análise.
 */
public record JobRecord(
        UUID jobId,
        String runId,
        JobClass jobClass,
        Duration size,
        Instant enqueueTs,
        Instant receiveTs,
        Instant startTs,
        Instant endTs,
        Duration serviceActual,
        String workerId,
        ConsumptionPolicy policy,
        QueueId queue,
        int receiveCount,
        UUID parentJobId,
        int sliceIndex,
        int sliceCount) {

    public JobRecord {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(jobClass, "jobClass");
        Objects.requireNonNull(size, "size");
        Objects.requireNonNull(enqueueTs, "enqueueTs");
        Objects.requireNonNull(receiveTs, "receiveTs");
        Objects.requireNonNull(startTs, "startTs");
        Objects.requireNonNull(endTs, "endTs");
        Objects.requireNonNull(serviceActual, "serviceActual");
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(parentJobId, "parentJobId");
    }

    public static JobRecord of(
            ReceivedMessage msg,
            WorkerSettings settings,
            Instant receiveTs,
            Instant startTs,
            Instant endTs,
            Duration serviceActual) {
        Job job = msg.job();
        return new JobRecord(
                job.jobId(), job.runId(), job.jobClass(), job.size(), job.enqueueTs(),
                receiveTs, startTs, endTs, serviceActual,
                settings.workerId(), settings.policy(), msg.queue(), msg.receiveCount(),
                job.parentJobId(), job.sliceIndex(), job.sliceCount());
    }
}
