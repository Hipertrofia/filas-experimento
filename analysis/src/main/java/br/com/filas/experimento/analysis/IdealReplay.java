package br.com.filas.experimento.analysis;

import br.com.filas.experimento.domain.JobClass;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.ToLongFunction;

/**
 * Fila ideal sobre a própria rodada: as mesmas chegadas e os mesmos tempos de serviço, a mesma
 * disciplina e nenhum overhead de entrega. A diferença entre o medido e esta réplica é o custo
 * da fila real naquele caminho amostral, sem o ruído de sorteio que a comparação com a fórmula
 * carrega.
 *
 * <p>Cada servidor tem as classes que pode atender, em ordem de preferência: todas, para FIFO e
 * prioridade compartilhados; uma só, nos pods dedicados de {@code FAST_LANE_FISICA} e
 * {@code RESERVADO}. Com {@code fifo}, o servidor pega a chegada mais antiga entre as suas
 * classes; sem, a primeira classe da lista com job esperando. Um job que chega com mais de um
 * servidor livre vai para o que está livre há mais tempo.
 *
 * <p>Com prioridade, a comparação por classe não é monotônica: um atraso pequeno de entrega pode
 * trocar a ordem (um prioritário que chega nesse intervalo passa na frente de um normal que, no
 * ideal, já tinha começado), e a razão de uma classe pode ficar abaixo de 1. A medida que não
 * depende da ordem é a conservação de Kleinrock, Σ ρₖ·E[Wqₖ], que o overhead só pode aumentar.
 */
final class IdealReplay {

    private IdealReplay() {
    }

    /** c servidores iguais, todos com todas as classes (prioritária primeiro). */
    static Map<UUID, Long> waits(List<RunData.JobOutcome> jobs, int servers, boolean priority,
                                 ToLongFunction<RunData.JobOutcome> serviceUs) {
        if (servers < 1) {
            throw new IllegalArgumentException("servers < 1: " + servers);
        }
        List<List<JobClass>> pools = Collections.nCopies(servers, List.of(JobClass.values()));
        return waits(jobs, pools, !priority, serviceUs);
    }

    /**
     * Espera ideal (µs) de cada job, por {@code parent_job_id}. Inclui os jobs do aquecimento.
     *
     * @param servers classes de cada servidor, em ordem de preferência
     */
    static Map<UUID, Long> waits(List<RunData.JobOutcome> jobs, List<List<JobClass>> servers, boolean fifo,
                                 ToLongFunction<RunData.JobOutcome> serviceUs) {
        if (servers.isEmpty()) {
            throw new IllegalArgumentException("nenhum servidor");
        }
        for (RunData.JobOutcome j : jobs) {
            if (servers.stream().noneMatch(s -> s.contains(j.jobClass()))) {
                throw new IllegalArgumentException("nenhum servidor atende " + j.jobClass());
            }
        }
        List<RunData.JobOutcome> arrivals = new ArrayList<>(jobs);
        arrivals.sort(Comparator.comparingLong(RunData.JobOutcome::enqueueUs));
        Map<JobClass, ArrayDeque<RunData.JobOutcome>> waiting = new EnumMap<>(JobClass.class);
        for (JobClass k : JobClass.values()) {
            waiting.put(k, new ArrayDeque<>());
        }
        int n = servers.size();
        long[] until = new long[n];
        PriorityQueue<Integer> busy = new PriorityQueue<>(Comparator.comparingLong((Integer s) -> until[s]));
        // Livres, do que está livre há mais tempo para o mais recente.
        TreeSet<Integer> idle = new TreeSet<>(Comparator.comparingLong((Integer s) -> until[s]).thenComparing(s -> s));
        for (int s = 0; s < n; s++) {
            until[s] = Long.MIN_VALUE;
            idle.add(s);
        }
        Map<UUID, Long> out = new HashMap<>();
        int next = 0;
        while (next < arrivals.size() || !busy.isEmpty()) {
            long arrival = next < arrivals.size() ? arrivals.get(next).enqueueUs() : Long.MAX_VALUE;
            long done = busy.isEmpty() ? Long.MAX_VALUE : until[busy.peek()];
            long t;
            // Chegada e término no mesmo instante: a chegada entra antes, e o servidor que termina já a vê.
            if (arrival <= done) {
                t = arrival;
                RunData.JobOutcome j = arrivals.get(next++);
                waiting.get(j.jobClass()).add(j);
            } else {
                t = done;
                idle.add(busy.poll());
            }
            for (Integer s : new ArrayList<>(idle)) {
                RunData.JobOutcome j = pick(servers.get(s), waiting, fifo);
                if (j == null) {
                    continue;
                }
                idle.remove(s);
                out.put(j.parentJobId(), t - j.enqueueUs());
                until[s] = t + serviceUs.applyAsLong(j);
                busy.add(s);
            }
        }
        return out;
    }

    private static RunData.JobOutcome pick(List<JobClass> classes, Map<JobClass, ArrayDeque<RunData.JobOutcome>> waiting,
                                           boolean fifo) {
        JobClass from = null;
        for (JobClass k : classes) {
            RunData.JobOutcome head = waiting.get(k).peek();
            if (head == null) {
                continue;
            }
            if (!fifo) {
                from = k;
                break;
            }
            if (from == null || head.enqueueUs() < waiting.get(from).peek().enqueueUs()) {
                from = k;
            }
        }
        return from == null ? null : waiting.get(from).poll();
    }
}
