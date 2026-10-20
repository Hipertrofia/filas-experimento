package br.com.filas.experimento.domain;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobTest {

    private static final UUID ID = new UUID(0, 1);
    private static final Instant T0 = Instant.parse("2026-10-20T12:00:00Z");
    private static final Duration SIZE = Duration.ofMillis(20);

    private static Job slice(int index, int count) {
        return new Job(ID, "run", JobClass.NORMAL, SIZE, T0, ID, index, count);
    }

    @Test
    void wholeNaoTemFatiamento() {
        Job j = Job.whole(ID, "run", JobClass.PRIORITY, SIZE, T0);
        assertEquals(ID, j.parentJobId());
        assertEquals(0, j.sliceIndex());
        assertEquals(1, j.sliceCount());
        assertFalse(j.hasNextSlice());
        assertThrows(IllegalStateException.class, () -> j.nextSlice(T0));
    }

    @Test
    void rejeitaValoresInvalidos() {
        assertThrows(IllegalArgumentException.class,
                () -> Job.whole(ID, "run", JobClass.NORMAL, Duration.ofMillis(-1), T0));
        assertThrows(IllegalArgumentException.class, () -> slice(0, 0));
        assertThrows(IllegalArgumentException.class, () -> slice(-1, 2));
        assertThrows(IllegalArgumentException.class, () -> slice(2, 2));
        assertThrows(NullPointerException.class,
                () -> Job.whole(null, "run", JobClass.NORMAL, SIZE, T0));
    }

    @Test
    void aceitaServicoZero() {
        // Rodadas de overhead do SQS usam serviço zero.
        assertEquals(Duration.ZERO, Job.whole(ID, "run", JobClass.NORMAL, Duration.ZERO, T0).size());
    }

    @Test
    void proximaFatiaTemIdDeterministicoEMantemOPai() {
        Job s0 = slice(0, 3);
        Instant later = T0.plusMillis(50);
        Job s1 = s0.nextSlice(later);

        assertTrue(s0.hasNextSlice());
        assertEquals(1, s1.sliceIndex());
        assertEquals(3, s1.sliceCount());
        assertEquals(ID, s1.parentJobId());
        assertEquals(SIZE, s1.size());
        assertEquals(later, s1.enqueueTs());
        assertEquals("run", s1.runId());
        assertEquals(JobClass.NORMAL, s1.jobClass());
        assertNotEquals(ID, s1.jobId());
        assertEquals(s1.jobId(), slice(0, 3).nextSlice(T0).jobId(), "mesmo id em qualquer execução");
        assertNotEquals(s1.jobId(), s1.nextSlice(later).jobId());
        assertFalse(s1.nextSlice(later).hasNextSlice());
    }
}
