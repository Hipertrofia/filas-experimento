package br.com.filas.experimento.domain;

import java.util.Objects;

/**
 * Mensagem entregue pela fila.
 *
 * @param receiptHandle identificador opaco usado pelo adaptador para confirmar a mensagem
 * @param receiveCount  número de entregas desta mensagem (maior que 1 indica reentrega)
 */
public record ReceivedMessage(Job job, QueueId queue, String receiptHandle, int receiveCount) {

    public ReceivedMessage {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(receiptHandle, "receiptHandle");
        if (receiveCount < 1) {
            throw new IllegalArgumentException("receiveCount < 1: " + receiveCount);
        }
    }
}
