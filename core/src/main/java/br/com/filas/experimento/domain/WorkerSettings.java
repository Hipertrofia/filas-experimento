package br.com.filas.experimento.domain;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * Configuração de um worker.
 *
 * @param workerId identifica pod e thread
 * @param queues   filas consultadas, em ordem de preferência
 */
public record WorkerSettings(String workerId, ConsumptionPolicy policy, List<QueueId> queues) {

    public WorkerSettings {
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(policy, "policy");
        queues = List.copyOf(Objects.requireNonNull(queues, "queues"));
        if (queues.isEmpty()) {
            throw new IllegalArgumentException("lista de filas vazia");
        }
        if (new HashSet<>(queues).size() != queues.size()) {
            throw new IllegalArgumentException("fila repetida: " + queues);
        }
    }
}
