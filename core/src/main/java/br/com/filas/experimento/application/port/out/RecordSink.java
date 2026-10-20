package br.com.filas.experimento.application.port.out;

import br.com.filas.experimento.domain.JobRecord;

/**
 * Destino dos registros. {@link #append} deve retornar sem bloquear em E/S (buffer em memória);
 * a escrita fica com outra thread, para não ocupar o servidor entre jobs.
 */
public interface RecordSink {

    void append(JobRecord record);
}
