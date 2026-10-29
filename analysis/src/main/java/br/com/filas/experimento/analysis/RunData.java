package br.com.filas.experimento.analysis;

import br.com.filas.experimento.domain.ConsumptionPolicy;
import br.com.filas.experimento.domain.JobClass;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Jobs concluídos de uma rodada, já deduplicados, com as fatias reunidas no job pai e o
 * aquecimento descartado. A janela de medição vai do fim do aquecimento à última chegada mantida.
 *
 * @param duplicates      atendimentos repetidos de um mesmo {@code job_id} (fica o que terminou primeiro)
 * @param incomplete      jobs pai com fatias faltando, fora da análise
 * @param negativeWaits   fatias com {@code start < enqueue}: sinal de desvio de relógio entre gerador e pod
 */
record RunData(
        Set<String> runIds,
        Set<ConsumptionPolicy> policies,
        Set<String> workers,
        int records,
        int duplicates,
        int incomplete,
        int warmupDiscarded,
        int negativeWaits,
        long windowStartUs,
        long windowEndUs,
        List<JobOutcome> jobs) {

    /**
     * Job pai. {@code waitUs} soma as esperas de todas as fatias e {@code serviceUs} os
     * atendimentos ({@code end − start}); W vai da chegada do pai ao fim da última fatia.
     */
    record JobOutcome(UUID parentJobId, JobClass jobClass, int sliceCount, long enqueueUs, long endUs,
                      long waitUs, long serviceUs, long nominalUs) {
        long sojournUs() {
            return endUs - enqueueUs;
        }
    }

    /** Um arquivo JSONL, ou um diretório cujos {@code *.jsonl} (um por pod) formam uma só rodada. */
    static RunData read(Path path, Duration warmup) throws IOException {
        List<Path> files;
        if (Files.isDirectory(path)) {
            try (Stream<Path> s = Files.list(path)) {
                files = s.filter(p -> p.getFileName().toString().endsWith(".jsonl")).sorted().toList();
            }
            if (files.isEmpty()) {
                throw new IllegalArgumentException(path + ": nenhum .jsonl no diretório");
            }
        } else {
            files = List.of(path);
        }
        List<Observation> obs = new ArrayList<>();
        for (Path f : files) {
            readInto(f, obs);
        }
        return of(obs, warmup);
    }

    private static void readInto(Path file, List<Observation> obs) throws IOException {
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            int n = 0;
            while ((line = r.readLine()) != null) {
                n++;
                if (line.isBlank()) {
                    continue;
                }
                try {
                    obs.add(Observation.parse(line));
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException(file + ":" + n + ": " + e.getMessage(), e);
                }
            }
        }
    }

    static RunData of(List<Observation> obs, Duration warmup) {
        if (obs.isEmpty()) {
            throw new IllegalArgumentException("registro vazio");
        }
        Set<String> runIds = new TreeSet<>();
        Set<ConsumptionPolicy> policies = new TreeSet<>();
        Set<String> workers = new TreeSet<>();
        Map<UUID, Observation> first = new HashMap<>();
        int duplicates = 0;
        for (Observation o : obs) {
            runIds.add(o.runId());
            policies.add(o.policy());
            workers.add(o.workerId());
            Observation prev = first.putIfAbsent(o.jobId(), o);
            if (prev != null) {
                duplicates++;
                if (o.endUs() < prev.endUs()) {
                    first.put(o.jobId(), o);
                }
            }
        }

        Map<UUID, List<Observation>> byParent = new HashMap<>();
        for (Observation o : first.values()) {
            byParent.computeIfAbsent(o.parentJobId(), k -> new ArrayList<>()).add(o);
        }
        List<JobOutcome> all = new ArrayList<>();
        int incomplete = 0;
        int negativeWaits = 0;
        for (Map.Entry<UUID, List<Observation>> e : byParent.entrySet()) {
            List<Observation> slices = e.getValue();
            slices.sort(Comparator.comparingInt(Observation::sliceIndex));
            int m = slices.get(0).sliceCount();
            boolean complete = slices.size() == m;
            for (int k = 0; complete && k < m; k++) {
                complete = slices.get(k).sliceIndex() == k && slices.get(k).sliceCount() == m;
            }
            if (!complete) {
                incomplete++;
                continue;
            }
            long wait = 0;
            long service = 0;
            long nominal = 0;
            for (Observation o : slices) {
                if (o.startUs() < o.enqueueUs()) {
                    negativeWaits++;
                }
                wait += o.startUs() - o.enqueueUs();
                service += o.endUs() - o.startUs();
                nominal += o.sizeUs();
            }
            Observation head = slices.get(0);
            all.add(new JobOutcome(e.getKey(), head.jobClass(), m, head.enqueueUs(),
                    slices.get(m - 1).endUs(), wait, service, nominal));
        }
        if (all.isEmpty()) {
            throw new IllegalArgumentException("nenhum job completo no registro");
        }

        long t0 = all.stream().mapToLong(JobOutcome::enqueueUs).min().orElseThrow();
        long cut = t0 + warmup.toNanos() / 1_000;
        List<JobOutcome> kept = all.stream()
                .filter(j -> j.enqueueUs() >= cut)
                .sorted(Comparator.comparingLong(JobOutcome::enqueueUs))
                .toList();
        if (kept.isEmpty()) {
            throw new IllegalArgumentException("o aquecimento de " + warmup + " descarta todos os jobs");
        }
        long end = kept.get(kept.size() - 1).enqueueUs();
        return new RunData(runIds, policies, workers, obs.size(), duplicates, incomplete,
                all.size() - kept.size(), negativeWaits, cut, end, kept);
    }

    double windowSeconds() {
        return (windowEndUs - windowStartUs) / 1e6;
    }

    boolean sliced() {
        return jobs.stream().anyMatch(j -> j.sliceCount() > 1);
    }
}
