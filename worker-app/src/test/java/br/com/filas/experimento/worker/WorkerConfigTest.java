package br.com.filas.experimento.worker;

import br.com.filas.experimento.domain.ConsumptionPolicy;
import br.com.filas.experimento.domain.JobClass;
import br.com.filas.experimento.domain.QueueId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerConfigTest {

    private static Map<String, String> base() {
        Map<String, String> m = new HashMap<>();
        m.put("worker.id", "pod-1");
        m.put("worker.policy", "prioridade");
        m.put("worker.queues", "prio, normal");
        return m;
    }

    @Test
    void padroes() {
        WorkerConfig c = WorkerConfig.from(base());

        assertEquals(ConsumptionPolicy.PRIORIDADE, c.policy());
        assertEquals(List.of(new QueueId("prio"), new QueueId("normal")), c.queues());
        assertEquals(List.of("pod-1/t0"), c.threadIds());
        assertEquals(WorkerConfig.ServiceMode.SLEEP, c.serviceMode());
        assertEquals(WorkerConfig.Backend.LOCAL, c.backend());
        assertEquals(Duration.ofSeconds(30), c.visibilityTimeout());
        assertEquals(Path.of("registros/pod-1.jsonl"), c.sinkFile());
        assertTrue(c.localTrace().isEmpty());
        assertFalse(c.exitWhenDrained());
        assertEquals(new QueueId("prio"), c.localQueueByClass().get(JobClass.PRIORITY));
        assertEquals(new QueueId("normal"), c.localQueueByClass().get(JobClass.NORMAL));
    }

    @Test
    void esperaPorFila() {
        Map<String, String> m = base();
        m.put("queue.wait-ms", "100");
        m.put("queue.wait-ms.prio", "0");
        WorkerConfig c = WorkerConfig.from(m);

        assertEquals(Duration.ofMillis(100), c.defaultWait());
        assertEquals(Map.of(new QueueId("prio"), Duration.ZERO), c.waitTimes());
    }

    @Test
    void rejeitaConfiguracoesIncoerentes() {
        Map<String, String> fifo = base();
        fifo.put("worker.policy", "FIFO_COMPARTILHADO");
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.from(fifo));

        Map<String, String> prio = base();
        prio.put("worker.queues", "prio");
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.from(prio));

        Map<String, String> wait = base();
        wait.put("queue.wait-ms.outra", "0");
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.from(wait));

        Map<String, String> semPolicy = base();
        semPolicy.remove("worker.policy");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> WorkerConfig.from(semPolicy));
        assertTrue(e.getMessage().contains("WORKER_POLICY"), e.getMessage());
    }

    @Test
    void precedenciaArquivoAmbienteSystemProperty(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("w.properties");
        Files.writeString(file, "worker.id=arquivo\nworker.policy=PRIORIDADE\nworker.queues=a,b\n"
                + "worker.threads=2\nservice.mode=SLEEP\n");
        Map<String, String> env = Map.of("WORKER_THREADS", "3", "SERVICE_MODE", "CPU_BURN");
        Properties sys = new Properties();
        sys.setProperty("service.mode", "SLEEP");
        sys.setProperty("queue.wait-ms.a", "5");
        sys.setProperty("java.version", "ignorada");

        WorkerConfig c = WorkerConfig.load(new String[] {file.toString()}, env, sys);

        assertEquals("arquivo", c.workerId());
        assertEquals(3, c.threads(), "ambiente vence o arquivo");
        assertEquals(WorkerConfig.ServiceMode.SLEEP, c.serviceMode(), "-D vence o ambiente");
        assertEquals(Duration.ofMillis(5), c.waitTimes().get(new QueueId("a")));
        assertEquals(List.of("arquivo/t0", "arquivo/t1", "arquivo/t2"), c.threadIds());
    }

    @Test
    void nomeDoArquivoPadraoNaoTemCaracteresInvalidos() {
        Map<String, String> m = base();
        m.put("worker.id", "ip-10-0-0-1/pod:a");
        assertEquals(Path.of("registros/ip-10-0-0-1_pod_a.jsonl"), WorkerConfig.from(m).sinkFile());
    }
}
