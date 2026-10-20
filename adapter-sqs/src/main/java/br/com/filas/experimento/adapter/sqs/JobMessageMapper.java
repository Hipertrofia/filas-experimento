package br.com.filas.experimento.adapter.sqs;

import br.com.filas.experimento.domain.Job;
import br.com.filas.experimento.domain.JobClass;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Corpo da mensagem SQS: objeto JSON plano com os campos da spec §2, tempos em microssegundos.
 *
 * <pre>
 * {"job_id":"…","run_id":"…","class":"NORMAL","size_us":187345,"enqueue_ts_us":1792497600123456,
 *  "parent_job_id":"…","slice_index":0,"slice_count":1}
 * </pre>
 *
 * O parser aceita só esse formato (strings e inteiros, sem aninhamento) e ignora campos extras.
 */
public final class JobMessageMapper {

    private JobMessageMapper() {
    }

    public static String toBody(Job job) {
        return "{\"job_id\":\"" + job.jobId()
                + "\",\"run_id\":" + quote(job.runId())
                + ",\"class\":\"" + job.jobClass().name()
                + "\",\"size_us\":" + micros(job.size())
                + ",\"enqueue_ts_us\":" + micros(job.enqueueTs())
                + ",\"parent_job_id\":\"" + job.parentJobId()
                + "\",\"slice_index\":" + job.sliceIndex()
                + ",\"slice_count\":" + job.sliceCount() + "}";
    }

    /** @throws IllegalArgumentException se o corpo não for um job válido */
    public static Job fromBody(String body) {
        Map<String, Object> f = new Parser(body).object();
        try {
            return new Job(
                    UUID.fromString(str(f, "job_id")),
                    str(f, "run_id"),
                    JobClass.valueOf(str(f, "class")),
                    Duration.ofNanos(Math.multiplyExact(num(f, "size_us"), 1_000L)),
                    instant(num(f, "enqueue_ts_us")),
                    UUID.fromString(str(f, "parent_job_id")),
                    Math.toIntExact(num(f, "slice_index")),
                    Math.toIntExact(num(f, "slice_count")));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("mensagem inválida: " + e.getMessage() + ": " + body, e);
        }
    }

    private static String str(Map<String, Object> f, String k) {
        if (!(f.get(k) instanceof String s)) {
            throw new IllegalArgumentException("campo string ausente: " + k);
        }
        return s;
    }

    private static long num(Map<String, Object> f, String k) {
        if (!(f.get(k) instanceof Long n)) {
            throw new IllegalArgumentException("campo inteiro ausente: " + k);
        }
        return n;
    }

    private static long micros(Duration d) {
        return Math.addExact(Math.multiplyExact(d.getSeconds(), 1_000_000L), d.getNano() / 1_000);
    }

    private static long micros(Instant t) {
        return Math.addExact(Math.multiplyExact(t.getEpochSecond(), 1_000_000L), t.getNano() / 1_000);
    }

    private static Instant instant(long micros) {
        return Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
    }

    private static String quote(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    /** Parser mínimo para um objeto JSON plano de strings e inteiros. */
    private static final class Parser {
        private final String s;
        private int i;

        Parser(String s) {
            this.s = s;
        }

        Map<String, Object> object() {
            Map<String, Object> m = new HashMap<>();
            skipWs();
            expect('{');
            skipWs();
            if (peek() == '}') {
                i++;
                return end(m);
            }
            while (true) {
                skipWs();
                String key = string();
                skipWs();
                expect(':');
                skipWs();
                m.put(key, peek() == '"' ? string() : number());
                skipWs();
                char c = next();
                if (c == '}') {
                    return end(m);
                }
                if (c != ',') {
                    throw error("esperado ',' ou '}'");
                }
            }
        }

        private Map<String, Object> end(Map<String, Object> m) {
            skipWs();
            if (i != s.length()) {
                throw error("conteúdo após o objeto");
            }
            return m;
        }

        private String string() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = next();
                if (c == '"') {
                    return sb.toString();
                }
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                char e = next();
                switch (e) {
                    case '"', '\\', '/' -> sb.append(e);
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'u' -> {
                        if (i + 4 > s.length()) {
                            throw error("escape \\u incompleto");
                        }
                        sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                        i += 4;
                    }
                    default -> throw error("escape inválido \\" + e);
                }
            }
        }

        private Long number() {
            int start = i;
            if (peek() == '-') {
                i++;
            }
            while (i < s.length() && Character.isDigit(s.charAt(i))) {
                i++;
            }
            if (start == i || (i == start + 1 && s.charAt(start) == '-')) {
                throw error("esperado string ou inteiro");
            }
            return Long.parseLong(s.substring(start, i));
        }

        private void skipWs() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }

        private char peek() {
            if (i >= s.length()) {
                throw error("fim inesperado");
            }
            return s.charAt(i);
        }

        private char next() {
            char c = peek();
            i++;
            return c;
        }

        private void expect(char c) {
            if (next() != c) {
                throw error("esperado '" + c + "'");
            }
        }

        private IllegalArgumentException error(String msg) {
            return new IllegalArgumentException(msg + " na posição " + i);
        }
    }
}
