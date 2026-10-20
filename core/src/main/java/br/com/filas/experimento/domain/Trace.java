package br.com.filas.experimento.domain;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Trace de chegadas ordenado por offset.
 *
 * @param metadata parâmetros que o geraram (informativo, gravado no cabeçalho do arquivo)
 */
public record Trace(String runId, long seed, Map<String, String> metadata, List<TraceEntry> entries) {

    public Trace {
        Objects.requireNonNull(runId, "runId");
        metadata = Collections.unmodifiableMap(new LinkedHashMap<>(metadata));
        entries = List.copyOf(entries);
        for (int i = 1; i < entries.size(); i++) {
            if (entries.get(i).offset().compareTo(entries.get(i - 1).offset()) < 0) {
                throw new IllegalArgumentException("trace fora de ordem na entrada " + i);
            }
        }
    }
}
