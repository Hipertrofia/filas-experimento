package br.com.filas.experimento.adapter.local;

import br.com.filas.experimento.application.port.out.JobQueue;
import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.ReceivedMessage;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Fila em memória com a semântica relevante do SQS: receive com espera (long polling),
 * visibility timeout e reentrega com {@code receiveCount} incrementado. Segura para vários
 * workers no mesmo processo (competing consumers).
 */
public final class InMemoryJobQueue implements JobQueue {

    private record Stored(Job job, int receiveCount) { }

    private record InFlight(Stored stored, QueueId queue, long visibleAtNanos) { }

    private final Map<QueueId, Duration> waitTimes;
    private final Duration defaultWaitTime;
    private final Duration visibilityTimeout;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Map<QueueId, Deque<Stored>> ready = new HashMap<>();
    private final Map<String, InFlight> inFlight = new HashMap<>();
    private long handles;

    /**
     * @param waitTimes         espera máxima do receive por fila (ex.: curta na prioritária)
     * @param defaultWaitTime   espera das filas fora do mapa; zero é polling curto
     * @param visibilityTimeout tempo até uma mensagem não confirmada voltar para a fila
     */
    public InMemoryJobQueue(
            Map<QueueId, Duration> waitTimes, Duration defaultWaitTime, Duration visibilityTimeout) {
        this.waitTimes = Map.copyOf(waitTimes);
        this.defaultWaitTime = Objects.requireNonNull(defaultWaitTime, "defaultWaitTime");
        this.visibilityTimeout = Objects.requireNonNull(visibilityTimeout, "visibilityTimeout");
    }

    public InMemoryJobQueue(Duration waitTime, Duration visibilityTimeout) {
        this(Map.of(), waitTime, visibilityTimeout);
    }

    @Override
    public Optional<ReceivedMessage> receive(QueueId queue) {
        long deadline = System.nanoTime() + waitTimes.getOrDefault(queue, defaultWaitTime).toNanos();
        lock.lock();
        try {
            while (true) {
                long now = System.nanoTime();
                requeueExpired(now);
                Stored stored = readyQueue(queue).pollFirst();
                if (stored != null) {
                    return Optional.of(deliver(stored, queue, now));
                }
                long remaining = deadline - now;
                if (remaining <= 0) {
                    return Optional.empty();
                }
                // Acorda também na expiração mais próxima, que pode devolver uma mensagem.
                changed.awaitNanos(Math.min(remaining, nanosUntilNextExpiry(now)));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    /** Remove a mensagem. Um handle já expirado (mensagem reentregue) é ignorado, como no SQS. */
    @Override
    public void acknowledge(ReceivedMessage message) {
        lock.lock();
        try {
            inFlight.remove(message.receiptHandle());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void send(Job job, QueueId queue) {
        lock.lock();
        try {
            readyQueue(queue).addLast(new Stored(job, 0));
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Mensagens prontas, sem contar as em voo. */
    public int readyCount(QueueId queue) {
        lock.lock();
        try {
            return readyQueue(queue).size();
        } finally {
            lock.unlock();
        }
    }

    public int inFlightCount() {
        lock.lock();
        try {
            return inFlight.size();
        } finally {
            lock.unlock();
        }
    }

    private ReceivedMessage deliver(Stored stored, QueueId queue, long now) {
        Stored delivered = new Stored(stored.job(), stored.receiveCount() + 1);
        String handle = queue + "#" + (++handles);
        inFlight.put(handle, new InFlight(delivered, queue, now + visibilityTimeout.toNanos()));
        return new ReceivedMessage(delivered.job(), queue, handle, delivered.receiveCount());
    }

    private void requeueExpired(long now) {
        Iterator<InFlight> it = inFlight.values().iterator();
        while (it.hasNext()) {
            InFlight f = it.next();
            if (f.visibleAtNanos() - now <= 0) {
                it.remove();
                readyQueue(f.queue()).addLast(f.stored());
            }
        }
    }

    private long nanosUntilNextExpiry(long now) {
        long min = Long.MAX_VALUE;
        for (InFlight f : inFlight.values()) {
            min = Math.min(min, Math.max(0, f.visibleAtNanos() - now));
        }
        return min == Long.MAX_VALUE ? TimeUnit.DAYS.toNanos(1) : Math.max(min, 1);
    }

    private Deque<Stored> readyQueue(QueueId queue) {
        return ready.computeIfAbsent(queue, q -> new ArrayDeque<>());
    }
}
