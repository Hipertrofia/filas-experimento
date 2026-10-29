package br.com.filas.experimento.analysis;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Leitor mínimo para as linhas do registro: objeto JSON plano, valores string ou número.
 * Números voltam como o texto original.
 */
final class JsonLine {

    private final String s;
    private int i;

    private JsonLine(String s) {
        this.s = s;
    }

    static Map<String, String> parse(String line) {
        return new JsonLine(line).object();
    }

    private Map<String, String> object() {
        Map<String, String> m = new LinkedHashMap<>();
        expect('{');
        if (peek() == '}') {
            i++;
            return end(m);
        }
        while (true) {
            String key = string();
            expect(':');
            String value = peek() == '"' ? string() : number();
            if (m.put(key, value) != null) {
                throw error("chave repetida \"" + key + "\"");
            }
            char c = next();
            if (c == '}') {
                return end(m);
            }
            if (c != ',') {
                throw error("esperava ',' ou '}'");
            }
        }
    }

    private Map<String, String> end(Map<String, String> m) {
        skipWs();
        if (i != s.length()) {
            throw error("conteúdo depois do objeto");
        }
        return m;
    }

    private String string() {
        expect('"');
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (i >= s.length()) {
                throw error("string sem fim");
            }
            char c = s.charAt(i++);
            if (c == '"') {
                return sb.toString();
            }
            if (c != '\\') {
                sb.append(c);
                continue;
            }
            if (i >= s.length()) {
                throw error("escape sem fim");
            }
            char e = s.charAt(i++);
            switch (e) {
                case '"', '\\', '/' -> sb.append(e);
                case 'b' -> sb.append('\b');
                case 'f' -> sb.append('\f');
                case 'n' -> sb.append('\n');
                case 'r' -> sb.append('\r');
                case 't' -> sb.append('\t');
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

    private String number() {
        int start = i;
        while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) {
            i++;
        }
        if (start == i) {
            throw error("valor inválido");
        }
        return s.substring(start, i);
    }

    private void expect(char c) {
        if (next() != c) {
            i--;
            throw error("esperava '" + c + "'");
        }
    }

    private char next() {
        skipWs();
        if (i >= s.length()) {
            throw error("fim inesperado");
        }
        return s.charAt(i++);
    }

    private char peek() {
        skipWs();
        return i < s.length() ? s.charAt(i) : '\0';
    }

    private void skipWs() {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
    }

    private IllegalArgumentException error(String msg) {
        return new IllegalArgumentException(msg + " na posição " + i);
    }
}
