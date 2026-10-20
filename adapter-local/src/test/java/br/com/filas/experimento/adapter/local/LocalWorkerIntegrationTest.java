package br.com.filas.experimento.adapter.local;

import br.com.filas.experimento.application.WorkerLoop;
import br.com.filas.experimento.domain.ConsumptionPolicy;
import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.QueueId;
import br.com.filas.experimento.domain.WorkerSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Dois workers concorrentes (competing consumers) com os adaptadores locais reais. */
class LocalWorkerIntegrationTest {

    private static final QueueId PRIO = new QueueId("prio");
    private static final QueueId NORMAL = new QueueId("normal");
    private static final Pattern JOB_ID = Pattern.compile("\"job_id\":\"([^\"]+)\"");
    private static final Pattern START = Pattern.compile("\"start_ts_us\":(\\d+)");
    private static final Pattern ENQUEUE = Pattern.compile("\"enqueue_ts_us\":(\\d+)");
    private static final Pattern END = Pattern.compile("\"end_ts_us\":(\\d+)");

    private static long field(Pattern p, String line) {
        Matcher m = p.matcher(line);
        assertTrue(m.find(), line);
        return Long.parseLong(m.group(1));
    }

    @Test
    void doisWorkersAtendemTodosOsJobsUmaVezCada(@TempDir Path dir) throws Exception {
        InMemoryJobQueue queue = new InMemoryJobQueue(
                java.util.Map.of(PRIO, Duration.ZERO), Duration.ofMillis(10), Duration.ofSeconds(30));
        SystemClockAdapter clock = new SystemClockAdapter();
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            JobClass c = i % 4 == 0 ? JobClass.PRIORITY : JobClass.NORMAL;
            Job job = Job.whole(UUID.randomUUID(), "run-it", c, Duration.ofMillis(c == JobClass.PRIORITY ? 2 : 10),
                    clock.wallNow());
            queue.send(job, c == JobClass.PRIORITY ? PRIO : NORMAL);
            sent.add(job.jobId().toString());
        }

        Path file = dir.resolve("r.jsonl");
        List<WorkerLoop> loops = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        try (BufferedJsonLinesSink sink = new BufferedJsonLinesSink(file, Duration.ofMillis(20))) {
            for (int w = 0; w < 2; w++) {
                WorkerLoop loop = new WorkerLoop(
                        new WorkerSettings("pod-" + w, ConsumptionPolicy.PRIORIDADE, List.of(PRIO, NORMAL)),
                        queue, new SleepServiceExecutor(), clock, sink);
                loops.add(loop);
                Thread t = new Thread(loop::run, "worker-" + w);
                threads.add(t);
                t.start();
            }
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while ((queue.readyCount(PRIO) + queue.readyCount(NORMAL) + queue.inFlightCount()) > 0
                    && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            loops.forEach(WorkerLoop::requestStop);
            for (Thread t : threads) {
                t.join(5_000);
            }
        }

        List<String> lines = Files.readAllLines(file);
        Set<String> served = new HashSet<>();
        for (String line : lines) {
            served.add(JOB_ID.matcher(line).results().findFirst().orElseThrow().group(1));
            assertTrue(field(START, line) >= field(ENQUEUE, line), line);
            assertTrue(field(END, line) >= field(START, line), line);
        }
        assertEquals(20, lines.size());
        assertEquals(sent, served);
    }
}
