package br.com.filas.experimento.application.fakes;

import br.com.filas.experimento.application.port.out.JobQueue;
import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.ReceivedMessage;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Filas em memória. Cada {@link #enqueue} do mesmo job conta como nova entrega, o que simula
 * a reentrega após o visibility timeout.
 */
public final class FakeJobQueue implements JobQueue {

    private final EventLog log;
    private final Map<QueueId, Deque<Job>> queues = new HashMap<>();
    private final Map<UUID, Integer> deliveries = new HashMap<>();
    private final Map<String, ReceivedMessage> inFlight = new HashMap<>();
    private final List<ReceivedMessage> acknowledged = new ArrayList<>();
    private final List<Sent> sent = new ArrayList<>();
    private Runnable whenAllEmpty = () -> { };
    private int handles;

    public record Sent(Job job, QueueId queue) { }

    public FakeJobQueue(EventLog log) {
        this.log = log;
    }

    public FakeJobQueue enqueue(QueueId queue, Job job) {
        queues.computeIfAbsent(queue, q -> new ArrayDeque<>()).addLast(job);
        return this;
    }

    /** Ação disparada quando um receive encontra todas as filas vazias (ex.: parar o loop). */
    public void whenAllEmpty(Runnable action) {
        this.whenAllEmpty = action;
    }

    @Override
    public Optional<ReceivedMessage> receive(QueueId queue) {
        Job job = queues.getOrDefault(queue, new ArrayDeque<>()).pollFirst();
        if (job == null) {
            log.add("receive " + queue + " vazio");
            if (queues.values().stream().allMatch(Deque::isEmpty)) {
                whenAllEmpty.run();
            }
            return Optional.empty();
        }
        int count = deliveries.merge(job.jobId(), 1, Integer::sum);
        ReceivedMessage msg = new ReceivedMessage(job, queue, "h" + (++handles), count);
        inFlight.put(msg.receiptHandle(), msg);
        log.add("receive " + queue + " " + job.jobId());
        return Optional.of(msg);
    }

    @Override
    public void acknowledge(ReceivedMessage message) {
        if (inFlight.remove(message.receiptHandle()) == null) {
            throw new IllegalStateException("ack de mensagem que não está em voo: " + message);
        }
        acknowledged.add(message);
        log.add("ack " + message.job().jobId());
    }

    @Override
    public void send(Job job, QueueId queue) {
        sent.add(new Sent(job, queue));
        enqueue(queue, job);
        log.add("send " + queue + " " + job.jobId());
    }

    public List<ReceivedMessage> acknowledged() {
        return List.copyOf(acknowledged);
    }

    public List<Sent> sent() {
        return List.copyOf(sent);
    }

    public int inFlightCount() {
        return inFlight.size();
    }
}
