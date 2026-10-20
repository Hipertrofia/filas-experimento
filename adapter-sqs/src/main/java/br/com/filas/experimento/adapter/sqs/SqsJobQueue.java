package br.com.filas.experimento.adapter.sqs;

import br.com.filas.experimento.application.port.out.JobQueue;
import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.ReceivedMessage;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Fila SQS. Um receive pede no máximo uma mensagem (sem prefetch) e define o visibility
 * timeout da entrega, para garantir que ele seja maior que o maior serviço possível.
 *
 * <p>No SQS, a espera do receive é em segundos inteiros (0 a 20). Com 0 (polling curto), o SQS
 * consulta só parte dos seus servidores e pode responder vazio mesmo com mensagens na fila.
 *
 * <p>Corpo malformado: a mensagem é ignorada sem ack e volta após o visibility timeout; uma
 * redrive policy com DLQ na fila evita que ela circule para sempre.
 */
public final class SqsJobQueue implements JobQueue, AutoCloseable {

    /**
     * @param waitTimes espera do long polling por fila; múltiplo de 1 s, de 0 a 20 s
     */
    public record Settings(Map<QueueId, Duration> waitTimes, Duration defaultWait, Duration visibilityTimeout) {

        public Settings {
            waitTimes = Map.copyOf(waitTimes);
            Objects.requireNonNull(defaultWait, "defaultWait");
            Objects.requireNonNull(visibilityTimeout, "visibilityTimeout");
            waitTimes.forEach((q, d) -> checkWait(q.name(), d));
            checkWait("padrão", defaultWait);
            long v = visibilityTimeout.toMillis();
            if (v < 1_000 || v % 1_000 != 0 || v > Duration.ofHours(12).toMillis()) {
                throw new IllegalArgumentException(
                        "visibility timeout do SQS: segundos inteiros entre 1 s e 12 h: " + visibilityTimeout);
            }
        }

        private static void checkWait(String q, Duration d) {
            long ms = d.toMillis();
            if (ms < 0 || ms > 20_000 || ms % 1_000 != 0) {
                throw new IllegalArgumentException(
                        "espera do SQS na fila " + q + ": segundos inteiros entre 0 e 20 s: " + d);
            }
        }

        int waitSeconds(QueueId q) {
            return (int) waitTimes.getOrDefault(q, defaultWait).toSeconds();
        }
    }

    private final SqsClient client;
    private final Settings settings;
    private final Map<QueueId, String> urls = new ConcurrentHashMap<>();
    private final Map<QueueId, LongAdder[]> counters = new ConcurrentHashMap<>();

    /** Receives feitos e quantos voltaram vazios, por fila: o custo em requisições da estratégia. */
    public record ReceiveStats(long receives, long empty) { }

    public SqsJobQueue(SqsClient client, Settings settings) {
        this.client = Objects.requireNonNull(client, "client");
        this.settings = Objects.requireNonNull(settings, "settings");
    }

    /**
     * Cliente SQS. Com {@code endpoint} (ElasticMQ, LocalStack), usa credenciais fixas de
     * desenvolvimento; sem ele, a cadeia padrão da AWS (perfil, variáveis, papel da instância).
     */
    public static SqsClient client(Optional<URI> endpoint, String region) {
        SqsClientBuilder b = SqsClient.builder().region(Region.of(region));
        if (endpoint.isPresent()) {
            b.endpointOverride(endpoint.get())
             .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")));
        } else {
            b.credentialsProvider(DefaultCredentialsProvider.builder().build());
        }
        return b.build();
    }

    /** Resolve as URLs das filas já na partida, para falhar cedo se alguma não existir. */
    public void resolve(List<QueueId> queues) {
        queues.forEach(this::url);
    }

    @Override
    public Optional<ReceivedMessage> receive(QueueId queue) {
        List<Message> msgs;
        try {
            msgs = client.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(url(queue))
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(settings.waitSeconds(queue))
                    .visibilityTimeout((int) settings.visibilityTimeout().toSeconds())
                    .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)
                    .build()).messages();
            count(queue, msgs.isEmpty());
        } catch (AbortedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (SdkClientException e) {
            if (interrupted(e)) {
                Thread.currentThread().interrupt();
                return Optional.empty();
            }
            throw e;
        }
        if (msgs.isEmpty()) {
            return Optional.empty();
        }
        Message m = msgs.get(0);
        Job job;
        try {
            job = JobMessageMapper.fromBody(m.body());
        } catch (IllegalArgumentException e) {
            System.err.println("[sqs] " + queue + ": mensagem " + m.messageId() + " ignorada: " + e.getMessage());
            return Optional.empty();
        }
        int count = Integer.parseInt(
                m.attributes().getOrDefault(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, "1"));
        return Optional.of(new ReceivedMessage(job, queue, m.receiptHandle(), count));
    }

    @Override
    public void acknowledge(ReceivedMessage message) {
        client.deleteMessage(b -> b.queueUrl(url(message.queue())).receiptHandle(message.receiptHandle()));
    }

    @Override
    public void send(Job job, QueueId queue) {
        client.sendMessage(b -> b.queueUrl(url(queue)).messageBody(JobMessageMapper.toBody(job)));
    }

    public Map<QueueId, ReceiveStats> receiveStats() {
        Map<QueueId, ReceiveStats> m = new java.util.TreeMap<>(java.util.Comparator.comparing(QueueId::name));
        counters.forEach((q, c) -> m.put(q, new ReceiveStats(c[0].sum(), c[1].sum())));
        return m;
    }

    private void count(QueueId queue, boolean empty) {
        LongAdder[] c = counters.computeIfAbsent(queue, q -> new LongAdder[] {new LongAdder(), new LongAdder()});
        c[0].increment();
        if (empty) {
            c[1].increment();
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private String url(QueueId queue) {
        return urls.computeIfAbsent(queue, q -> client.getQueueUrl(b -> b.queueName(q.name())).queueUrl());
    }

    private static boolean interrupted(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof InterruptedException) {
                return true;
            }
        }
        return Thread.currentThread().isInterrupted();
    }
}
