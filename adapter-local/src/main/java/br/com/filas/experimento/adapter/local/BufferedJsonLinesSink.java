package br.com.filas.experimento.adapter.local;

import br.com.filas.experimento.application.port.out.RecordSink;
import br.com.filas.experimento.domain.JobRecord;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

/**
 * Sink JSON Lines: {@link #append} só coloca o registro num buffer em memória sem limite e
 * retorna; uma thread separada grava e faz flush a cada intervalo. {@link #close()} descarrega
 * o que restou. Deve ser fechado depois de o worker parar.
 *
 * <p>Falha de E/S não é silenciosa: o próximo {@code append} e o {@code close} a propagam.
 */
public final class BufferedJsonLinesSink implements RecordSink, AutoCloseable {

    private final ConcurrentLinkedQueue<JobRecord> buffer = new ConcurrentLinkedQueue<>();
    private final BufferedWriter out;
    private final long flushIntervalNanos;
    private final Thread flusher;

    private volatile boolean closed;
    private volatile IOException failure;

    public BufferedJsonLinesSink(Path file, Duration flushInterval) throws IOException {
        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        this.out = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        this.flushIntervalNanos = flushInterval.toNanos();
        this.flusher = new Thread(this::flushLoop, "jsonl-flusher");
        this.flusher.setDaemon(true);
        this.flusher.start();
    }

    @Override
    public void append(JobRecord record) {
        if (closed) {
            throw new IllegalStateException("sink fechado");
        }
        IOException f = failure;
        if (f != null) {
            throw new UncheckedIOException("falha ao gravar registros", f);
        }
        buffer.add(record);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        LockSupport.unpark(flusher);
        try {
            flusher.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            out.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void flushLoop() {
        while (!closed && failure == null) {
            LockSupport.parkNanos(flushIntervalNanos);
            drain();
        }
        drain();
    }

    private void drain() {
        if (failure != null) {
            return;
        }
        try {
            JobRecord r;
            boolean wrote = false;
            while ((r = buffer.poll()) != null) {
                out.write(JobRecordJson.toJson(r));
                out.write('\n');
                wrote = true;
            }
            if (wrote) {
                out.flush();
            }
        } catch (IOException e) {
            failure = e;
        }
    }
}
