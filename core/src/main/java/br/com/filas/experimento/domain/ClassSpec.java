package br.com.filas.experimento.domain;

import java.time.Duration;
import java.util.Objects;

/**
 * Parâmetros de uma classe no gerador.
 *
 * @param ratePerSecond taxa de chegada λ (Poisson)
 * @param meanSize      média do tempo de serviço do job inteiro (exponencial)
 * @param sliceCount    número de fatias de cada job; o serviço total é dividido igualmente
 */
public record ClassSpec(double ratePerSecond, Duration meanSize, int sliceCount) {

    public ClassSpec {
        Objects.requireNonNull(meanSize, "meanSize");
        if (!Double.isFinite(ratePerSecond) || ratePerSecond < 0) {
            throw new IllegalArgumentException("taxa inválida: " + ratePerSecond);
        }
        if (meanSize.isNegative()) {
            throw new IllegalArgumentException("média negativa: " + meanSize);
        }
        if (sliceCount < 1) {
            throw new IllegalArgumentException("sliceCount < 1: " + sliceCount);
        }
    }

    /** Carga oferecida pela classe, em servidores: λ·E[S]. */
    public double offeredLoad() {
        return ratePerSecond * meanSize.toNanos() / 1e9;
    }
}
