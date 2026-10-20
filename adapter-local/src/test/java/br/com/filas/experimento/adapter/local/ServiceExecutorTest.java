package br.com.filas.experimento.adapter.local;

import br.com.filas.experimento.application.port.out.ServiceExecutor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Teste 1 da spec (precisão do atendimento). Os limites são folgados porque dependem da máquina;
 * os números medidos vão para a saída padrão.
 */
class ServiceExecutorTest {

    private static final int AMOSTRAS = 15;

    private static void medir(String nome, ServiceExecutor ex, long nominalMs, long maxMediaErroUs)
            throws InterruptedException {
        long nominal = nominalMs * 1_000_000L;
        long soma = 0;
        long max = 0;
        for (int i = 0; i < AMOSTRAS; i++) {
            long t0 = System.nanoTime();
            ex.serve(Duration.ofMillis(nominalMs));
            long erro = System.nanoTime() - t0 - nominal;
            assertTrue(erro >= 0, nome + " retornou antes do nominal: " + erro + " ns");
            soma += erro;
            max = Math.max(max, erro);
        }
        long mediaUs = soma / AMOSTRAS / 1_000;
        System.out.printf("%s %d ms: erro médio %d µs, máximo %d µs%n", nome, nominalMs, mediaUs, max / 1_000);
        assertTrue(mediaUs < maxMediaErroUs, nome + " erro médio alto: " + mediaUs + " µs");
    }

    @Test
    void sleepNuncaRetornaAntesDoNominal() throws InterruptedException {
        medir("SLEEP", new SleepServiceExecutor(), 20, 20_000);
        medir("SLEEP", new SleepServiceExecutor(), 200, 20_000);
    }

    @Test
    void cpuBurnEPreciso() throws InterruptedException {
        medir("CPU_BURN", new CpuBurnServiceExecutor(), 20, 2_000);
        medir("CPU_BURN", new CpuBurnServiceExecutor(), 200, 2_000);
    }

    @Test
    void servicoZeroRetornaNaHora() throws InterruptedException {
        new SleepServiceExecutor().serve(Duration.ZERO);
        new CpuBurnServiceExecutor().serve(Duration.ZERO);
    }

    @Test
    void cpuBurnRespondeAInterrupcao() throws Exception {
        AtomicReference<Throwable> erro = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                new CpuBurnServiceExecutor().serve(Duration.ofSeconds(30));
            } catch (Throwable e) {
                erro.set(e);
            }
        });
        t.start();
        Thread.sleep(50);
        t.interrupt();
        t.join(2_000);

        assertTrue(!t.isAlive(), "CPU_BURN não parou");
        assertInstanceOf(InterruptedException.class, erro.get());
    }
}
