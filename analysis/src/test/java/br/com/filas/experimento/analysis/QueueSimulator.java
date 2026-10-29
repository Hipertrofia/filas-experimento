package br.com.filas.experimento.analysis;

import br.com.filas.experimento.domain.ConsumptionPolicy;
import br.com.filas.experimento.domain.JobClass;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.UUID;

/**
 * Simulador de referência: chegadas de Poisson e serviço exponencial por classe, c servidores,
 * FIFO ou prioridade não-preemptiva. Produz observações no formato do worker para testar a
 * análise de ponta a ponta contra as fórmulas.
 */
final class QueueSimulator {

    record ClassParams(double ratePerS, double meanMs) { }

    private record Arrival(long id, JobClass jobClass, double t, double size) { }

    private static final long EPOCH_US = 1_792_500_000_000_000L;

    private QueueSimulator() {
    }

    static List<Observation> run(Map<JobClass, ClassParams> classes, int servers, ConsumptionPolicy policy,
                                 double horizonS, long seed) {
        Random rnd = new Random(seed);
        List<Arrival> arrivals = new ArrayList<>();
        long id = 0;
        for (Map.Entry<JobClass, ClassParams> e : new EnumMap<>(classes).entrySet()) {
            double t = 0;
            while (true) {
                t += exp(rnd, 1 / e.getValue().ratePerS());
                if (t > horizonS) {
                    break;
                }
                arrivals.add(new Arrival(id++, e.getKey(), t, exp(rnd, e.getValue().meanMs() / 1e3)));
            }
        }
        arrivals.sort(Comparator.comparingDouble(Arrival::t));

        Comparator<Arrival> order = policy == ConsumptionPolicy.PRIORIDADE
                ? Comparator.comparing(Arrival::jobClass).thenComparingDouble(Arrival::t)
                : Comparator.comparingDouble(Arrival::t);
        PriorityQueue<Arrival> waiting = new PriorityQueue<>(order);
        PriorityQueue<double[]> free = new PriorityQueue<>(Comparator.comparingDouble(s -> s[0]));
        for (int s = 0; s < servers; s++) {
            free.add(new double[] {0, s});
        }
        List<Observation> out = new ArrayList<>(arrivals.size());
        int next = 0;
        while (next < arrivals.size() || !waiting.isEmpty()) {
            double[] server = free.poll();
            double at = server[0];
            while (next < arrivals.size() && arrivals.get(next).t() <= at) {
                waiting.add(arrivals.get(next++));
            }
            Arrival job;
            double start;
            if (waiting.isEmpty()) {
                job = arrivals.get(next++);
                start = job.t();
            } else {
                job = waiting.poll();
                start = at;
            }
            double end = start + job.size();
            free.add(new double[] {end, server[1]});
            out.add(observation(job, start, end, "s" + (int) server[1], policy));
        }
        return out;
    }

    private static Observation observation(Arrival a, double start, double end, String worker,
                                           ConsumptionPolicy policy) {
        UUID id = new UUID(0, a.id());
        long sizeUs = us(a.size());
        long startUs = EPOCH_US + us(start);
        return new Observation(id, "sim", a.jobClass(), sizeUs, EPOCH_US + us(a.t()), startUs, startUs,
                EPOCH_US + us(end), sizeUs, worker, policy, a.jobClass().name(), 1, id, 0, 1);
    }

    private static long us(double seconds) {
        return Math.round(seconds * 1e6);
    }

    private static double exp(Random rnd, double mean) {
        return -mean * Math.log(1 - rnd.nextDouble());
    }
}
