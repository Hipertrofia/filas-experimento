package br.com.filas.experimento.analysis;

import br.com.filas.experimento.domain.ConsumptionPolicy;
import br.com.filas.experimento.domain.JobClass;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.UUID;
import java.util.function.ToLongFunction;

/**
 * Métricas por classe de uma rodada, com a previsão teórica alimentada pelos λ e momentos de
 * serviço medidos na própria rodada (assim a comparação isola a disciplina, não o sorteio).
 */
record RunReport(
        Path file,
        RunData data,
        int servers,
        ServiceBasis basis,
        Optional<Theory.Model> model,
        String modelNote,
        double rho,
        Map<JobClass, ClassResult> classes,
        Map<JobClass, Theory.Prediction> theory,
        Map<JobClass, ClassIdeal> ideal) {

    /** Serviço usado na teoria: o medido ({@code end − start}) ou o nominal do trace. */
    enum ServiceBasis { MEASURED, NOMINAL }

    enum ModelChoice { AUTO, FIFO, PRIORITY, NONE }

    record ClassResult(JobClass jobClass, double lambdaPerS, Sample service, Sample wq, Sample w) { }

    /** Wq e W da réplica ideal ({@link IdealReplay}) para os mesmos jobs. */
    record ClassIdeal(Sample wq, Sample w) { }

    static RunReport of(Path file, RunData data, Optional<Integer> servers, ServiceBasis basis, ModelChoice choice) {
        int c = servers.orElse(data.workers().size());
        double window = data.windowSeconds();
        Map<JobClass, ClassResult> classes = new EnumMap<>(JobClass.class);
        List<Theory.ClassInput> inputs = new ArrayList<>();
        double load = 0;
        for (JobClass k : JobClass.values()) {
            List<RunData.JobOutcome> js = data.jobs().stream().filter(j -> j.jobClass() == k).toList();
            if (js.isEmpty()) {
                continue;
            }
            Sample service = ms(js, basis == ServiceBasis.MEASURED
                    ? RunData.JobOutcome::serviceUs : RunData.JobOutcome::nominalUs);
            double lambda = window > 0 ? js.size() / window : Double.NaN;
            classes.put(k, new ClassResult(k, lambda, service,
                    ms(js, RunData.JobOutcome::waitUs), ms(js, RunData.JobOutcome::sojournUs)));
            inputs.add(new Theory.ClassInput(k, lambda / 1e3, service.mean(), service.secondMoment(),
                    service.thirdMoment()));
            load += lambda * service.mean() / 1e3;
        }
        double rho = load / c;

        Optional<Theory.Model> model = Optional.empty();
        String note;
        if (data.sliced() && choice == ModelChoice.AUTO) {
            note = "sem fórmula para jobs fatiados";
        } else if (choice == ModelChoice.NONE) {
            note = "teoria desligada (--model none)";
        } else if (choice == ModelChoice.FIFO || choice == ModelChoice.PRIORITY) {
            model = Optional.of(choice == ModelChoice.FIFO ? Theory.Model.FIFO : Theory.Model.PRIORITY);
            note = model.get().label(c);
        } else if (data.policies().equals(Set.of(ConsumptionPolicy.FIFO_COMPARTILHADO))) {
            model = Optional.of(Theory.Model.FIFO);
            note = model.get().label(c);
        } else if (data.policies().equals(Set.of(ConsumptionPolicy.PRIORIDADE))) {
            model = Optional.of(Theory.Model.PRIORITY);
            note = model.get().label(c);
        } else if (data.policies().equals(Set.of(ConsumptionPolicy.FAST_LANE_FISICA)) && dedicatedPools(data)) {
            // Pools disjuntos: cada classe é uma fila FIFO independente com os seus pods.
            model = Optional.of(Theory.Model.FIFO);
            note = "FIFO por pool, cada classe com os seus pods:";
            for (JobClass k : classes.keySet()) {
                int pool = poolSize(data, k);
                note += " " + k + " c=" + pool + " (" + Theory.Model.FIFO.label(pool) + ")";
            }
        } else if (data.policies().equals(Set.of(ConsumptionPolicy.RESERVADO))) {
            note = "sem fórmula fechada para RESERVADO; compare com a réplica ideal";
        } else {
            note = "sem fórmula para a política " + data.policies() + " (use --model)";
        }
        Map<JobClass, Theory.Prediction> theory = new EnumMap<>(JobClass.class);
        if (model.isPresent() && note.startsWith("FIFO por pool")) {
            for (Theory.ClassInput in : inputs) {
                theory.putAll(Theory.predict(Theory.Model.FIFO, List.of(in), poolSize(data, in.jobClass())));
            }
        } else if (model.isPresent()) {
            theory.putAll(Theory.predict(model.get(), inputs, c));
        }
        if (model.isPresent() && theory.size() < inputs.size()) {
            note += String.format(Locale.ROOT, "; instável com ρ = %.3f", rho);
        }
        return new RunReport(file, data, c, basis, model, note, rho, classes, theory, ideal(data, c, basis, choice));
    }

    /** Cada worker atende uma classe só, e toda classe tem pods. */
    private static boolean dedicatedPools(RunData data) {
        return data.workerClasses().values().stream().allMatch(s -> s.size() == 1)
                && data.jobs().stream().allMatch(j -> poolSize(data, j.jobClass()) > 0);
    }

    private static int poolSize(RunData data, JobClass k) {
        return (int) data.workerClasses().values().stream().filter(s -> s.equals(Set.of(k))).count();
    }

    /** Todos os pods atendem todas as classes que aparecem: a conservação de Kleinrock vale. */
    boolean sharedPool() {
        return data.workerClasses().values().stream().allMatch(s -> s.containsAll(classes.keySet()));
    }

    /**
     * Réplica ideal com a disciplina da rodada: FIFO entre as classes em {@code FIFO_COMPARTILHADO},
     * prioritária primeiro nas demais. Os pods são os do registro, cada um com as classes que
     * atendeu; com {@code --servers} diferente do número de workers, c pods iguais.
     */
    private static Map<JobClass, ClassIdeal> ideal(RunData data, int c, ServiceBasis basis, ModelChoice choice) {
        if (data.sliced() || choice == ModelChoice.NONE || data.policies().size() != 1) {
            return Map.of();
        }
        boolean fifo = choice == ModelChoice.FIFO
                || (choice == ModelChoice.AUTO && data.policies().contains(ConsumptionPolicy.FIFO_COMPARTILHADO));
        List<List<JobClass>> pools = data.workerClasses().size() == c
                ? data.workerClasses().values().stream().map(List::copyOf).toList()
                : Collections.nCopies(c, List.of(JobClass.values()));
        ToLongFunction<RunData.JobOutcome> service = basis == ServiceBasis.MEASURED
                ? RunData.JobOutcome::serviceUs : RunData.JobOutcome::nominalUs;
        Map<UUID, Long> waits = IdealReplay.waits(data.allJobs(), pools, fifo, service);
        Map<JobClass, ClassIdeal> out = new EnumMap<>(JobClass.class);
        for (JobClass k : JobClass.values()) {
            List<RunData.JobOutcome> js = data.jobs().stream().filter(j -> j.jobClass() == k).toList();
            if (js.isEmpty()) {
                continue;
            }
            out.put(k, new ClassIdeal(
                    Sample.of(js.stream().mapToDouble(j -> waits.get(j.parentJobId()) / 1e3).toArray()),
                    Sample.of(js.stream()
                            .mapToDouble(j -> (waits.get(j.parentJobId()) + service.applyAsLong(j)) / 1e3).toArray())));
        }
        return out;
    }

    private static Sample ms(List<RunData.JobOutcome> js, ToLongFunction<RunData.JobOutcome> f) {
        return Sample.of(js.stream().mapToDouble(j -> f.applyAsLong(j) / 1e3).toArray());
    }

    String render() {
        RunData d = data;
        StringBuilder sb = new StringBuilder();
        sb.append("== ").append(file).append('\n');
        sb.append(String.format(Locale.ROOT, "run_id %s | política %s | c = %d (%d workers no registro) | serviço %s%n",
                String.join(",", d.runIds()), d.policies(), servers, d.workers().size(),
                basis == ServiceBasis.MEASURED ? "medido (end − start)" : "nominal (size_us)"));
        sb.append(String.format(Locale.ROOT,
                "registros %d | duplicatas %d | incompletos %d | descartados no aquecimento %d | jobs %d em %.1f s"
                        + " | ρ = %.3f%n",
                d.records(), d.duplicates(), d.incomplete(), d.warmupDiscarded(), d.jobs().size(),
                d.windowSeconds(), rho));
        if (d.negativeWaits() > 0) {
            sb.append("ATENÇÃO: ").append(d.negativeWaits())
              .append(" fatias com start < enqueue: confira o desvio de relógio entre gerador e pods\n");
        }
        if (d.runIds().size() > 1) {
            sb.append("ATENÇÃO: mais de um run_id no mesmo arquivo\n");
        }
        Map<Set<JobClass>, Long> pools = new LinkedHashMap<>();
        d.workerClasses().values().stream().sorted(Comparator.comparingInt(Set::size))
                .forEach(s -> pools.merge(s, 1L, Long::sum));
        sb.append("pods por classes atendidas:");
        pools.forEach((s, n) -> sb.append(' ').append(n).append("× ").append(s));
        sb.append('\n');
        sb.append(String.format(Locale.ROOT, "%n%-8s %-6s %7s %8s %9s %9s %10s %12s %9s %9s %12s %9s%n",
                "classe", "", "n", "λ (1/s)", "E[S]", "E[S²]", "E[Wq]", "E[Wq²]", "p99 Wq", "E[W]", "E[W²]",
                "p99 W"));
        for (ClassResult r : classes.values()) {
            sb.append(String.format(Locale.ROOT, "%-8s %-6s %7d %8.3f %9.2f %9.0f %10.2f %12.0f %9.1f %9.2f %12.0f %9.1f%n",
                    r.jobClass(), "medido", r.wq().n(), r.lambdaPerS(), r.service().mean(), r.service().secondMoment(),
                    r.wq().mean(), r.wq().secondMoment(), r.wq().p99(), r.w().mean(), r.w().secondMoment(),
                    r.w().p99()));
            ClassIdeal id = ideal.get(r.jobClass());
            if (id != null) {
                sb.append(String.format(Locale.ROOT, "%-8s %-6s %7s %8s %9s %9s %10.2f %12.0f %9.1f %9.2f %12.0f %9.1f%n",
                        "", "ideal", "", "", "", "", id.wq().mean(), id.wq().secondMoment(), id.wq().p99(),
                        id.w().mean(), id.w().secondMoment(), id.w().p99()));
                sb.append(String.format(Locale.ROOT, "%-8s %-6s %7s %8s %9s %9s %10.3f %12.3f %9.3f %9.3f %12.3f %9.3f%n",
                        "", "÷ideal", "", "", "", "", r.wq().mean() / id.wq().mean(),
                        r.wq().secondMoment() / id.wq().secondMoment(), r.wq().p99() / id.wq().p99(),
                        r.w().mean() / id.w().mean(), r.w().secondMoment() / id.w().secondMoment(),
                        r.w().p99() / id.w().p99()));
            }
            Theory.Prediction p = theory.get(r.jobClass());
            if (p != null) {
                sb.append(String.format(Locale.ROOT, "%-8s %-6s %7s %8s %9s %9s %10.2f %12s %9s %9.2f %12s %9s%n",
                        "", "teoria", "", "", "", "", p.wq(), opt(p.wq2(), "%.0f"), "", p.w(), opt(p.w2(), "%.0f"), ""));
                sb.append(String.format(Locale.ROOT, "%-8s %-6s %7s %8s %9s %9s %10.3f %12s %9s %9.3f %12s %9s%n",
                        "", "razão", "", "", "", "", r.wq().mean() / p.wq(),
                        opt(ratio(r.wq().secondMoment(), p.wq2()), "%.3f"), "",
                        r.w().mean() / p.w(), opt(ratio(r.w().secondMoment(), p.w2()), "%.3f"), ""));
            }
        }
        sb.append("tempos em ms; teoria: ").append(modelNote);
        if (!ideal.isEmpty()) {
            sb.append("; ideal: mesmas chegadas e serviços, sem overhead de entrega");
        }
        sb.append('\n');
        if (ideal.keySet().equals(classes.keySet()) && !ideal.isEmpty() && sharedPool()) {
            double measured = 0;
            double replay = 0;
            for (ClassResult r : classes.values()) {
                double rhoK = r.lambdaPerS() * r.service().mean() / 1e3;
                measured += rhoK * r.wq().mean();
                replay += rhoK * ideal.get(r.jobClass()).wq().mean();
            }
            sb.append(String.format(Locale.ROOT,
                    "conservação Σ ρₖ·E[Wqₖ] (não depende da ordem): medido %.2f, ideal %.2f, razão %.3f%n",
                    measured, replay, measured / replay));
        }
        Delivery dl = d.delivery();
        sb.append("entrega, fila ocupada (fim → início do próximo):");
        for (Map.Entry<JobClass, Sample> e : dl.handoff().entrySet()) {
            sb.append(String.format(Locale.ROOT, " %s n=%d média %.2f mediana %.2f p99 %.2f ms;", e.getKey(),
                    e.getValue().n(), e.getValue().mean(), e.getValue().p50(), e.getValue().p99()));
        }
        Sample idle = dl.idlePickup();
        sb.append(String.format(Locale.ROOT,
                "%nentrega, servidor ocioso (chegada → início): n=%d média %.2f mediana %.2f p99 %.2f ms%n",
                idle.n(), idle.mean(), idle.p50(), idle.p99()));
        return sb.toString();
    }

    private static OptionalDouble ratio(double measured, OptionalDouble predicted) {
        return predicted.isPresent() ? OptionalDouble.of(measured / predicted.getAsDouble()) : OptionalDouble.empty();
    }

    private static String opt(OptionalDouble v, String fmt) {
        return v.isPresent() ? String.format(Locale.ROOT, fmt, v.getAsDouble()) : "—";
    }

    static final String CSV_HEADER = "file,run_id,policy,servers,service_basis,model,rho,class,n,lambda_per_s,"
            + "mean_s_ms,m2_s_ms2,mean_wq_ms,m2_wq_ms2,p50_wq_ms,p95_wq_ms,p99_wq_ms,"
            + "mean_w_ms,m2_w_ms2,p50_w_ms,p95_w_ms,p99_w_ms,"
            + "theory_wq_ms,theory_wq2_ms2,theory_w_ms,theory_w2_ms2,"
            + "ideal_wq_ms,ideal_wq2_ms2,ideal_p99_wq_ms,ideal_w_ms,"
            + "handoff_mean_ms,handoff_p50_ms,handoff_p99_ms,idle_pickup_p50_ms,idle_pickup_p99_ms";

    List<String> csvRows() {
        List<String> rows = new ArrayList<>();
        for (ClassResult r : classes.values()) {
            Theory.Prediction p = theory.get(r.jobClass());
            ClassIdeal id = ideal.get(r.jobClass());
            Sample h = data.delivery().handoff().get(r.jobClass());
            rows.add(String.join(",",
                    csvText(file.toString()), csvText(String.join(";", data.runIds())),
                    csvText(data.policies().stream().map(Enum::name).reduce((a, b) -> a + ";" + b).orElse("")),
                    Integer.toString(servers), basis.name(), model.map(Enum::name).orElse(""), num(rho),
                    r.jobClass().name(), Integer.toString(r.wq().n()), num(r.lambdaPerS()),
                    num(r.service().mean()), num(r.service().secondMoment()),
                    num(r.wq().mean()), num(r.wq().secondMoment()), num(r.wq().p50()), num(r.wq().p95()),
                    num(r.wq().p99()),
                    num(r.w().mean()), num(r.w().secondMoment()), num(r.w().p50()), num(r.w().p95()),
                    num(r.w().p99()),
                    p == null ? "" : num(p.wq()), p == null ? "" : opt(p.wq2()),
                    p == null ? "" : num(p.w()), p == null ? "" : opt(p.w2()),
                    id == null ? "" : num(id.wq().mean()), id == null ? "" : num(id.wq().secondMoment()),
                    id == null ? "" : num(id.wq().p99()), id == null ? "" : num(id.w().mean()),
                    h == null ? "" : num(h.mean()), h == null ? "" : num(h.p50()), h == null ? "" : num(h.p99()),
                    num(data.delivery().idlePickup().p50()), num(data.delivery().idlePickup().p99())));
        }
        return rows;
    }

    private static String num(double v) {
        return String.format(Locale.ROOT, "%.6g", v);
    }

    private static String opt(OptionalDouble v) {
        return v.isPresent() ? num(v.getAsDouble()) : "";
    }

    private static String csvText(String s) {
        return s.contains(",") || s.contains("\"") ? "\"" + s.replace("\"", "\"\"") + "\"" : s;
    }
}
