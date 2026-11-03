package br.com.filas.experimento.analysis;

import br.com.filas.experimento.domain.JobClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Overhead de entrega da fila, medido na sequência de atendimentos de cada worker (em ms):
 * <ul>
 *   <li>{@code handoff}: fila ocupada. O próximo job já esperava quando o anterior terminou, então
 *       {@code start(próximo) − end(anterior)} é só o custo de confirmar e receber. Por classe do
 *       próximo job: com duas filas consultadas em ordem, receber da segunda custa mais.</li>
 *   <li>{@code idlePickup}: servidor ocioso. O job chegou depois de o worker terminar o anterior,
 *       então {@code start − enqueue} é o atraso para um servidor livre ver a chegada.</li>
 * </ul>
 * Vale para qualquer c: entre os dois atendimentos, aquele worker não fez mais nada. Só entram
 * os atendimentos cujo job chegou depois do aquecimento.
 */
record Delivery(Map<JobClass, Sample> handoff, Sample idlePickup) {

    static Delivery of(Collection<Observation> obs, long cutUs) {
        Map<String, List<Observation>> byWorker = obs.stream()
                .collect(Collectors.groupingBy(Observation::workerId));
        Map<JobClass, List<Double>> handoff = new EnumMap<>(JobClass.class);
        List<Double> idle = new ArrayList<>();
        for (List<Observation> seq : byWorker.values()) {
            seq.sort(Comparator.comparingLong(Observation::startUs));
            for (int i = 0; i < seq.size(); i++) {
                Observation b = seq.get(i);
                if (b.enqueueUs() < cutUs) {
                    continue;
                }
                Observation a = i == 0 ? null : seq.get(i - 1);
                if (a != null && b.enqueueUs() < a.endUs()) {
                    handoff.computeIfAbsent(b.jobClass(), k -> new ArrayList<>()).add((b.startUs() - a.endUs()) / 1e3);
                } else {
                    idle.add((b.startUs() - b.enqueueUs()) / 1e3);
                }
            }
        }
        Map<JobClass, Sample> h = new EnumMap<>(JobClass.class);
        handoff.forEach((k, v) -> h.put(k, sample(v)));
        return new Delivery(h, sample(idle));
    }

    private static Sample sample(List<Double> v) {
        return Sample.of(v.stream().mapToDouble(Double::doubleValue).toArray());
    }
}
