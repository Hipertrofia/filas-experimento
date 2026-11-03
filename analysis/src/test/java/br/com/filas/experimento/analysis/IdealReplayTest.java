package br.com.filas.experimento.analysis;

import br.com.filas.experimento.domain.ConsumptionPolicy;
import br.com.filas.experimento.domain.JobClass;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IdealReplayTest {

    private static final long T0 = 1_792_500_000_000_000L;

    private static final Map<JobClass, QueueSimulator.ClassParams> CLASSES = Map.of(
            JobClass.PRIORITY, new QueueSimulator.ClassParams(8, 20),
            JobClass.NORMAL, new QueueSimulator.ClassParams(8, 200));

    /** O simulador de referência não tem overhead: a réplica ideal tem que dar as mesmas esperas. */
    private static void reproduzOSimulador(ConsumptionPolicy policy, int servers) {
        List<Observation> obs = QueueSimulator.run(CLASSES, servers, policy, 300, 7);
        RunData d = RunData.of(obs, Duration.ZERO);
        Map<UUID, Long> waits = IdealReplay.waits(d.allJobs(), servers, policy == ConsumptionPolicy.PRIORIDADE,
                RunData.JobOutcome::serviceUs);
        for (RunData.JobOutcome j : d.jobs()) {
            // Tolerância de arredondamento: o simulador grava instantes em µs.
            assertEquals(j.waitUs(), waits.get(j.parentJobId()), 2 * servers, "job " + j.parentJobId());
        }
    }

    @Test
    void fifoComUmETresServidores() {
        reproduzOSimulador(ConsumptionPolicy.FIFO_COMPARTILHADO, 1);
        reproduzOSimulador(ConsumptionPolicy.FIFO_COMPARTILHADO, 3);
    }

    @Test
    void prioridadeComUmETresServidores() {
        reproduzOSimulador(ConsumptionPolicy.PRIORIDADE, 1);
        reproduzOSimulador(ConsumptionPolicy.PRIORIDADE, 3);
    }

    @Test
    void poolsDisjuntosSaoFilasIndependentes() {
        RunData d = RunData.of(QueueSimulator.run(CLASSES, 2, ConsumptionPolicy.PRIORIDADE, 300, 11), Duration.ZERO);
        Map<UUID, Long> juntos = IdealReplay.waits(d.allJobs(),
                List.of(List.of(JobClass.PRIORITY), List.of(JobClass.NORMAL), List.of(JobClass.NORMAL)), false,
                RunData.JobOutcome::serviceUs);
        for (JobClass k : JobClass.values()) {
            List<RunData.JobOutcome> so = d.allJobs().stream().filter(j -> j.jobClass() == k).toList();
            Map<UUID, Long> separado = IdealReplay.waits(so, k == JobClass.PRIORITY ? 1 : 2, false,
                    RunData.JobOutcome::serviceUs);
            so.forEach(j -> assertEquals(separado.get(j.parentJobId()), juntos.get(j.parentJobId()), k + " " + j.parentJobId()));
        }
    }

    private static RunData.JobOutcome job(long n, JobClass c, long enqueue, long service) {
        return new RunData.JobOutcome(new UUID(0, n), c, 1, enqueue, enqueue + service, 0, service, service);
    }

    @Test
    void reservadoAjudaANormalEDeixaOPrioritarioEsperarOTermino() {
        // Pod 0 reservado (prioritária, depois normal); pod 1 só normal.
        List<RunData.JobOutcome> jobs = List.of(
                job(1, JobClass.NORMAL, 0, 100),      // pod 0 (livre há mais tempo, desempate pelo índice)
                job(2, JobClass.NORMAL, 10, 100),     // pod 1
                job(3, JobClass.PRIORITY, 20, 10),    // espera o pod 0 terminar em 100
                job(4, JobClass.NORMAL, 30, 50));     // espera o pod 1 terminar em 110
        Map<UUID, Long> w = IdealReplay.waits(jobs,
                List.of(List.of(JobClass.PRIORITY, JobClass.NORMAL), List.of(JobClass.NORMAL)), false,
                RunData.JobOutcome::serviceUs);
        assertEquals(0L, w.get(new UUID(0, 1)));
        assertEquals(0L, w.get(new UUID(0, 2)));
        assertEquals(80L, w.get(new UUID(0, 3)));
        assertEquals(80L, w.get(new UUID(0, 4)));
    }

    private static Observation obs(long n, JobClass c, String worker, long enqueue, long start, long end) {
        UUID id = new UUID(0, n);
        return new Observation(id, "r", c, end - start, T0 + enqueue, T0 + start, T0 + start, T0 + end,
                end - start, worker, ConsumptionPolicy.PRIORIDADE, "q", 1, id, 0, 1);
    }

    @Test
    void entregaSeparaFilaOcupadaDeServidorOcioso() {
        RunData d = RunData.of(List.of(
                // w1: o job 2 esperava quando o 1 terminou (handoff de 3 ms); o 3 chegou com w1 livre.
                obs(1, JobClass.NORMAL, "w1", 0, 1_000, 100_000),
                obs(2, JobClass.PRIORITY, "w1", 50_000, 103_000, 120_000),
                obs(3, JobClass.NORMAL, "w1", 300_000, 302_000, 400_000),
                // w2: um só job, servidor ocioso.
                obs(4, JobClass.NORMAL, "w2", 10_000, 15_000, 20_000)), Duration.ZERO);
        Delivery dl = d.delivery();
        assertEquals(1, dl.handoff().get(JobClass.PRIORITY).n());
        assertEquals(3.0, dl.handoff().get(JobClass.PRIORITY).mean(), 1e-9);
        assertEquals(null, dl.handoff().get(JobClass.NORMAL));
        assertEquals(3, dl.idlePickup().n());
        assertEquals((1.0 + 2.0 + 5.0) / 3, dl.idlePickup().mean(), 1e-9);
    }
}
