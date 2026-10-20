package br.com.filas.experimento.adapter.sqs;

import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.JobClass;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobMessageMapperTest {

    private static final UUID ID = UUID.fromString("6ec20ffa-6a4a-379b-8fe3-0d2d6d2377b2");
    private static final UUID PARENT = UUID.fromString("00000000-0000-0000-0000-000000000009");

    @Test
    void idaEVoltaComMicrossegundos() {
        Job j = new Job(ID, "rho080 \"s1\"\\x", JobClass.NORMAL, Duration.ofNanos(187_345_000),
                Instant.parse("2026-10-20T12:00:00.123456Z"), PARENT, 2, 4);

        String body = JobMessageMapper.toBody(j);

        assertEquals("{\"job_id\":\"6ec20ffa-6a4a-379b-8fe3-0d2d6d2377b2\",\"run_id\":\"rho080 \\\"s1\\\"\\\\x\","
                + "\"class\":\"NORMAL\",\"size_us\":187345,\"enqueue_ts_us\":1792497600123456,"
                + "\"parent_job_id\":\"00000000-0000-0000-0000-000000000009\",\"slice_index\":2,\"slice_count\":4}", body);
        assertEquals(j, JobMessageMapper.fromBody(body));
    }

    @Test
    void toleraEspacosOrdemECamposExtras() {
        String body = " { \"slice_count\" : 1 , \"extra\" : \"x\\u00e9\" , \"slice_index\":0,"
                + "\"parent_job_id\":\"" + ID + "\",\"enqueue_ts_us\":0,\"size_us\":0,"
                + "\"class\":\"PRIORITY\",\"run_id\":\"r\",\"job_id\":\"" + ID + "\" } ";

        Job j = JobMessageMapper.fromBody(body);

        assertEquals(Job.whole(ID, "r", JobClass.PRIORITY, Duration.ZERO, Instant.EPOCH), j);
    }

    @Test
    void rejeitaCorposInvalidos() {
        String valido = JobMessageMapper.toBody(Job.whole(ID, "r", JobClass.NORMAL, Duration.ofMillis(1), Instant.EPOCH));
        for (String body : new String[] {
                "", "não é json", "{", "[]", valido + "x",
                valido.replace("\"NORMAL\"", "\"URGENTE\""),
                valido.replace("\"size_us\":1000", "\"size_us\":\"1000\""),
                valido.replace(",\"slice_count\":1", ""),
                valido.replace("\"slice_index\":0", "\"slice_index\":5")}) {
            assertThrows(IllegalArgumentException.class, () -> JobMessageMapper.fromBody(body), body);
        }
    }
}
