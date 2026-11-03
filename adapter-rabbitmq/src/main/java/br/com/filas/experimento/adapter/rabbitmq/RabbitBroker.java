package br.com.filas.experimento.adapter.rabbitmq;

import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.QueueId;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * Conexão com o RabbitMQ. Cada worker recebe o seu {@link RabbitJobQueue} (um canal próprio,
 * prefetch = 1); o gerador usa um só para publicar.
 *
 * <p>A disciplina fica na declaração da fila: as filas em {@code priorityQueues} são declaradas
 * com {@code x-max-priority = 1} e entregam primeiro os jobs {@link JobClass#PRIORITY}; as demais
 * são FIFO. Gerador e workers declaram as mesmas filas com os mesmos argumentos; se a fila já
 * existir com outros, o broker recusa e a partida falha.
 *
 * <p>Sem recuperação automática da conexão: se o broker cair, a rodada falha em vez de seguir
 * com entregas refeitas.
 */
public final class RabbitBroker implements AutoCloseable {

    static final int MAX_PRIORITY = 1;

    private final Connection connection;
    private final Set<QueueId> priorityQueues;

    private RabbitBroker(Connection connection, Set<QueueId> priorityQueues) {
        this.connection = connection;
        this.priorityQueues = Set.copyOf(priorityQueues);
    }

    /** @param name nome da conexão, visível no painel do broker */
    public static RabbitBroker connect(URI uri, Set<QueueId> priorityQueues, String name) {
        Objects.requireNonNull(uri, "uri");
        ConnectionFactory f = new ConnectionFactory();
        try {
            f.setUri(uri);
        } catch (Exception e) {
            throw new IllegalArgumentException("URI do RabbitMQ inválida: " + uri, e);
        }
        f.setAutomaticRecoveryEnabled(false);
        f.setTopologyRecoveryEnabled(false);
        try {
            return new RabbitBroker(f.newConnection(name), priorityQueues);
        } catch (IOException e) {
            throw new UncheckedIOException("sem conexão com o RabbitMQ em " + uri.getHost() + ":" + uri.getPort(), e);
        } catch (TimeoutException e) {
            throw new IllegalStateException("tempo esgotado conectando ao RabbitMQ em " + uri.getHost(), e);
        }
    }

    /** Declara as filas (idempotente). Falha se alguma já existir com argumentos diferentes. */
    public void declare(Collection<QueueId> queues) {
        for (QueueId q : queues) {
            Map<String, Object> args = new HashMap<>();
            args.put("x-queue-type", "classic");
            if (priorityQueues.contains(q)) {
                args.put("x-max-priority", MAX_PRIORITY);
            }
            // Canal descartável: uma declaração recusada fecha o canal.
            try (Channel ch = connection.createChannel()) {
                ch.queueDeclare(q.name(), true, false, false, args);
            } catch (IOException e) {
                throw new IllegalStateException("fila " + q + " não pôde ser declarada com " + args
                        + " (já existe com outros argumentos?)", e);
            } catch (TimeoutException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /** Fila de um worker: consome {@code queue} por push, esperando até {@code wait} em cada receive. */
    public RabbitJobQueue consumer(QueueId queue, Duration wait) {
        return new RabbitJobQueue(channel(), List.of(queue), wait);
    }

    /** Fila de um worker que consulta várias filas em ordem, por pull ({@code basic.get}). */
    public RabbitJobQueue poller(List<QueueId> queues) {
        if (queues.size() < 2) {
            throw new IllegalArgumentException("pull é para duas ou mais filas; com uma, use consumer: " + queues);
        }
        return new RabbitJobQueue(channel(), queues, Duration.ZERO);
    }

    /** Fila só para envio (gerador). */
    public RabbitJobQueue publisher() {
        return new RabbitJobQueue(channel(), List.of(), Duration.ZERO);
    }

    static int priorityOf(JobClass c) {
        return c == JobClass.PRIORITY ? MAX_PRIORITY : 0;
    }

    private Channel channel() {
        try {
            return connection.createChannel();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (connection.isOpen()) {
                connection.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
