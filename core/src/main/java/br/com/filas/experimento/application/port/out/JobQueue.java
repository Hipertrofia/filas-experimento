package br.com.filas.experimento.application.port.out;

import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.ReceivedMessage;

import java.util.Optional;

/**
 * Acesso mínimo à fila. Quanto tempo {@link #receive} espera (polling curto ou longo) é
 * configuração do adaptador.
 */
public interface JobQueue {

    /** No máximo uma mensagem, sem prefetch. */
    Optional<ReceivedMessage> receive(QueueId queue);

    /** Confirma (delete). Chamado só depois de atender. */
    void acknowledge(ReceivedMessage message);

    /** Envia para o fim da fila. Usado no fatiamento. */
    void send(Job job, QueueId queue);
}
