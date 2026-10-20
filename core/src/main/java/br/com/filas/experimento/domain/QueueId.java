package br.com.filas.experimento.domain;

import java.util.Objects;

/** Fila lógica. O adaptador traduz o nome para o recurso real (URL do SQS, fila em memória). */
public record QueueId(String name) {

    public QueueId {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name vazio");
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
