package br.com.filas.experimento.domain;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkerSettingsTest {

    private static final QueueId A = new QueueId("a");
    private static final QueueId B = new QueueId("b");

    @Test
    void rejeitaListaVaziaOuComRepeticao() {
        assertThrows(IllegalArgumentException.class,
                () -> new WorkerSettings("w", ConsumptionPolicy.PRIORIDADE, List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new WorkerSettings("w", ConsumptionPolicy.PRIORIDADE, List.of(A, B, A)));
        assertThrows(IllegalArgumentException.class, () -> new QueueId(" "));
    }

    @Test
    void copiaAListaEPreservaAOrdem() {
        List<QueueId> queues = new ArrayList<>(List.of(B, A));
        WorkerSettings s = new WorkerSettings("w", ConsumptionPolicy.PRIORIDADE, queues);
        queues.clear();
        assertEquals(List.of(B, A), s.queues());
    }
}
