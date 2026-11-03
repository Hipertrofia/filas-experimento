package br.com.filas.experimento.adapter.rabbitmq;

import br.com.filas.experimento.application.JobMessageMapper;
import br.com.filas.experimento.application.port.out.JobQueue;
import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.ReceivedMessage;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Fila RabbitMQ de um worker: {@code basic.consume} com prefetch = 1 num canal próprio. O broker
 * empurra a próxima entrega assim que a anterior é confirmada, sem polling; numa fila com
 * {@code x-max-priority}, a escolhida é a de maior prioridade naquele instante. É prioridade
 * não-preemptiva, como em Cobham.
 *
 * <p>Com prefetch = 1 o pod nunca tem mais de uma mensagem não confirmada, a que está sendo
 * atendida ou a que acabou de chegar e espera o {@link #receive}. {@code receive} só espera essa
 * entrega por até {@code wait}; o valor não afeta a medição, só a rapidez com que o worker vê um
 * pedido de parada.
 *
 * <p>Com mais de uma fila (o pod reservado de {@code RESERVADO}), o worker passa a fazer pull:
 * {@code basic.get} em cada fila, na ordem do {@code WorkerLoop}, como no SQS. Consumir as duas
 * por push no mesmo canal deixaria o broker escolher entre elas, sem ordem de preferência. O
 * {@code basic.get} não espera: {@code worker.idle-pause-ms} limita a taxa quando todas vêm vazias.
 *
 * <p>{@code receive_count}: o RabbitMQ só informa se a mensagem é reentrega, então vale 1 ou 2.
 * Corpo malformado: a mensagem é rejeitada sem requeue (senão, com prefetch = 1, travaria o canal).
 * Envios usam publisher confirms, conferidos em {@link #close()}.
 */
public final class RabbitJobQueue implements JobQueue, AutoCloseable {

    private static final Duration CONFIRM_TIMEOUT = Duration.ofSeconds(10);

    private record Delivery(Envelope envelope, byte[] body) { }

    private final Channel channel;
    private final List<QueueId> consumed;
    private final boolean pull;
    private final long waitNanos;
    private final BlockingQueue<Delivery> deliveries = new LinkedBlockingQueue<>();
    private volatile String failure;
    private boolean consuming;

    RabbitJobQueue(Channel channel, List<QueueId> consumed, Duration wait) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.consumed = List.copyOf(consumed);
        this.pull = this.consumed.size() > 1;
        if (wait.isNegative()) {
            throw new IllegalArgumentException("espera negativa: " + wait);
        }
        this.waitNanos = wait.toNanos();
        // Envio para uma fila que não existe (nome errado) volta como basic.return.
        channel.addReturnListener(r -> System.err.println("[rabbitmq] envio sem fila de destino: "
                + r.getRoutingKey() + " (" + r.getReplyText() + ")"));
        io(channel::confirmSelect);
    }

    @Override
    public Optional<ReceivedMessage> receive(QueueId queue) {
        if (!consumed.contains(queue)) {
            throw new IllegalArgumentException("este worker consome " + (consumed.isEmpty() ? "nenhuma fila" : consumed)
                    + ", não " + queue);
        }
        Delivery d = pull ? get(queue) : next(queue);
        if (d == null) {
            return Optional.empty();
        }
        long tag = d.envelope().getDeliveryTag();
        Job job;
        try {
            job = JobMessageMapper.fromBody(new String(d.body(), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            System.err.println("[rabbitmq] " + queue + ": entrega " + tag + " rejeitada: " + e.getMessage());
            io(() -> channel.basicReject(tag, false));
            return Optional.empty();
        }
        return Optional.of(new ReceivedMessage(job, queue, Long.toString(tag), d.envelope().isRedeliver() ? 2 : 1));
    }

    @Override
    public void acknowledge(ReceivedMessage message) {
        io(() -> channel.basicAck(Long.parseLong(message.receiptHandle()), false));
    }

    @Override
    public void send(Job job, QueueId queue) {
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(1)
                .priority(RabbitBroker.priorityOf(job.jobClass()))
                .messageId(job.jobId().toString())
                .timestamp(Date.from(job.enqueueTs()))
                .build();
        byte[] body = JobMessageMapper.toBody(job).getBytes(StandardCharsets.UTF_8);
        io(() -> channel.basicPublish("", queue.name(), true, props, body));
    }

    /**
     * Espera as confirmações dos envios e fecha o canal. Entregas ainda não confirmadas (a que
     * esperava o receive, ou um job interrompido) voltam para a fila.
     *
     * @throws IllegalStateException se algum envio não foi confirmado pelo broker
     */
    @Override
    public void close() {
        if (!channel.isOpen()) {
            return;
        }
        try {
            channel.waitForConfirmsOrDie(CONFIRM_TIMEOUT.toMillis());
        } catch (IOException | TimeoutException e) {
            throw new IllegalStateException("envios sem confirmação do broker", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            try {
                channel.close();
            } catch (IOException | TimeoutException | ShutdownSignalException ignored) {
                // canal já encerrado pelo broker
            }
        }
    }

    private Delivery get(QueueId queue) {
        GetResponse r;
        try {
            r = channel.basicGet(queue.name(), false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return r == null ? null : new Delivery(r.getEnvelope(), r.getBody());
    }

    private Delivery next(QueueId queue) {
        startConsuming(queue);
        Delivery d;
        try {
            d = deliveries.poll(waitNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
        if (d == null && failure != null) {
            throw new IllegalStateException("consumo de " + queue + " encerrado: " + failure);
        }
        return d;
    }

    private void startConsuming(QueueId queue) {
        if (consuming) {
            return;
        }
        io(() -> {
            channel.basicQos(1);
            channel.basicConsume(queue.name(), false, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) {
                    deliveries.add(new Delivery(envelope, body));
                }

                @Override
                public void handleCancel(String consumerTag) {
                    failure = "consumidor cancelado pelo broker (fila removida?)";
                }

                @Override
                public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                    if (!sig.isInitiatedByApplication()) {
                        failure = sig.getMessage();
                    }
                }
            });
        });
        consuming = true;
    }

    private interface IoAction {
        void run() throws IOException;
    }

    private static void io(IoAction a) {
        try {
            a.run();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
