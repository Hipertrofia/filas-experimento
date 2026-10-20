package br.com.filas.experimento.domain;

/**
 * Disciplina do experimento. Só rotula o registro: o comportamento do worker vem da lista
 * ordenada de filas em {@link WorkerSettings}. O {@code k} de {@code RESERVADO(k)} é decisão de
 * implantação (quantos pods recebem cada lista).
 */
public enum ConsumptionPolicy {
    FIFO_COMPARTILHADO,
    PRIORIDADE,
    RESERVADO,
    FAST_LANE_FISICA
}
