package br.com.filas.experimento.application.fakes;

import br.com.filas.experimento.application.port.out.RecordSink;
import br.com.filas.experimento.domain.JobRecord;

import java.util.ArrayList;
import java.util.List;

public final class ListRecordSink implements RecordSink {

    private final EventLog log;
    private final List<JobRecord> records = new ArrayList<>();

    public ListRecordSink(EventLog log) {
        this.log = log;
    }

    @Override
    public void append(JobRecord record) {
        records.add(record);
        log.add("record " + record.jobId());
    }

    public List<JobRecord> records() {
        return List.copyOf(records);
    }
}
