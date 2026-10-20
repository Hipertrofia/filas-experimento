package br.com.filas.experimento.application.fakes;

import java.util.ArrayList;
import java.util.List;

/** Log compartilhado entre os fakes, para conferir a ordem das chamadas. */
public final class EventLog {

    private final List<String> events = new ArrayList<>();

    public void add(String event) {
        events.add(event);
    }

    public List<String> events() {
        return List.copyOf(events);
    }

    public long count(String prefix) {
        return events.stream().filter(e -> e.startsWith(prefix)).count();
    }
}
