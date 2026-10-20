package br.com.filas.experimento.domain;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * Parâmetros de um trace: chegadas de Poisson por classe em {@code [0, horizon)}, tamanhos
 * exponenciais, tudo determinado por {@code seed}.
 */
public record TraceSpec(String runId, long seed, Duration horizon, Map<JobClass, ClassSpec> classes) {

    public TraceSpec {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(horizon, "horizon");
        if (runId.isBlank()) {
            throw new IllegalArgumentException("runId vazio");
        }
        if (horizon.isNegative() || horizon.isZero()) {
            throw new IllegalArgumentException("horizonte deve ser positivo: " + horizon);
        }
        classes = Map.copyOf(classes);
        if (classes.isEmpty()) {
            throw new IllegalArgumentException("nenhuma classe");
        }
    }

    /**
     * Taxas a partir da utilização alvo: λ = ρ·c / E[S], com
     * E[S] = p·E[S_prio] + (1 − p)·E[S_normal] e p a fração de jobs prioritários.
     */
    public static TraceSpec fromUtilization(
            String runId, long seed, Duration horizon,
            double rho, int servers, double priorityFraction,
            Duration meanPriority, Duration meanNormal, int slicesPriority, int slicesNormal) {
        if (!(rho > 0) || servers < 1 || !(priorityFraction >= 0 && priorityFraction <= 1)) {
            throw new IllegalArgumentException(
                    "rho > 0, servers >= 1 e 0 <= p <= 1: " + rho + ", " + servers + ", " + priorityFraction);
        }
        double meanSeconds = (priorityFraction * meanPriority.toNanos()
                + (1 - priorityFraction) * meanNormal.toNanos()) / 1e9;
        if (!(meanSeconds > 0)) {
            throw new IllegalArgumentException("E[S] = 0: use taxas explícitas (rodada de overhead)");
        }
        double lambda = rho * servers / meanSeconds;
        Map<JobClass, ClassSpec> classes = new EnumMap<>(JobClass.class);
        classes.put(JobClass.PRIORITY, new ClassSpec(priorityFraction * lambda, meanPriority, slicesPriority));
        classes.put(JobClass.NORMAL, new ClassSpec((1 - priorityFraction) * lambda, meanNormal, slicesNormal));
        return new TraceSpec(runId, seed, horizon, classes);
    }

    /** Carga oferecida total em servidores (Σ λ_k E[S_k]); dividida por c dá ρ. */
    public double offeredLoad() {
        return classes.values().stream().mapToDouble(ClassSpec::offeredLoad).sum();
    }
}
