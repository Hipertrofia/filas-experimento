package br.com.filas.experimento.adapter.sqs;

import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;
import java.util.Optional;
import java.util.UUID;

/** ElasticMQ embutido para os testes: servidor SQS compatível numa porta livre. */
final class ElasticMq implements AutoCloseable {

    private final SQSRestServer server;
    private final URI endpoint;

    ElasticMq() {
        server = SQSRestServerBuilder.withDynamicPort().start();
        int port = server.waitUntilStarted().localAddress().getPort();
        endpoint = URI.create("http://localhost:" + port);
    }

    URI endpoint() {
        return endpoint;
    }

    SqsClient client() {
        return SqsJobQueue.client(Optional.of(endpoint), "us-east-1");
    }

    /** Cria uma fila com nome único e devolve o nome. */
    static String createQueue(SqsClient client, String prefix) {
        String name = prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
        client.createQueue(b -> b.queueName(name));
        return name;
    }

    @Override
    public void close() {
        server.stopAndWait();
    }
}
