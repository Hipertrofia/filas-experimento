package br.com.filas.experimento.analysis;

import br.com.filas.experimento.domain.JobClass;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.function.ToLongFunction;

/**
 * Fila ideal sobre a própria rodada: as mesmas chegadas e os mesmos tempos de serviço, c
 * servidores, FIFO ou prioridade não-preemptiva, e nenhum overhead de entrega. A diferença
 * entre o medido e esta réplica é o custo da fila real naquele caminho amostral, sem o ruído
 * de sorteio que a comparação com a fórmula carrega.
 *
 * <p>Com prioridade, a comparação por classe não é monotônica: um atraso pequeno de entrega pode
 * trocar a ordem (um prioritário que chega nesse intervalo passa na frente de um normal que, no
 * ideal, já tinha começado), e a razão de uma classe pode ficar abaixo de 1. A medida que não
 * depende da ordem é a conservação de Kleinrock, Σ ρₖ·E[Wqₖ], que o overhead só pode aumentar.
 */
final class IdealReplay {

    private IdealReplay() {
    }

    /** Espera ideal (µs) de cada job, por {@code parent_job_id}. Inclui os jobs do aquecimento. */
    static Map<UUID, Long> waits(List<RunData.JobOutcome> jobs, int servers, boolean priority,
                                 ToLongFunction<RunData.JobOutcome> serviceUs) {
        if (servers < 1) {
            throw new IllegalArgumentException("servers < 1: " + servers);
        }
        List<RunData.JobOutcome> arrivals = jobs.stream()
                .sorted(Comparator.comparingLong(RunData.JobOutcome::enqueueUs))
                .toList();
        Comparator<RunData.JobOutcome> fifo = Comparator.comparingLong(RunData.JobOutcome::enqueueUs);
        Comparator<RunData.JobOutcome> order = priority
                ? Comparator.comparing((RunData.JobOutcome j) -> j.jobClass() == JobClass.PRIORITY ? 0 : 1).thenComparing(fifo)
                : fifo;
        PriorityQueue<RunData.JobOutcome> waiting = new PriorityQueue<>(order);
        // Instante em que cada servidor fica livre.
        PriorityQueue<Long> free = new PriorityQueue<>();
        for (int s = 0; s < servers; s++) {
            free.add(Long.MIN_VALUE);
        }
        Map<UUID, Long> out = new HashMap<>();
        int next = 0;
        while (next < arrivals.size() || !waiting.isEmpty()) {
            long at = free.poll();
            while (next < arrivals.size() && arrivals.get(next).enqueueUs() <= at) {
                waiting.add(arrivals.get(next++));
            }
            RunData.JobOutcome job;
            long start;
            if (waiting.isEmpty()) {
                job = arrivals.get(next++);
                start = job.enqueueUs();
            } else {
                job = waiting.poll();
                start = at;
            }
            out.put(job.parentJobId(), start - job.enqueueUs());
            free.add(start + serviceUs.applyAsLong(job));
        }
        return out;
    }
}
