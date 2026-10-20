package br.com.filas.experimento.adapter.local;

import br.com.filas.experimento.application.port.out.ServiceExecutor;

import java.time.Duration;

/**
 * Modo {@code CPU_BURN}: ocupa um núcleo em laço até o prazo monotônico. Sem
 * {@code Thread.onSpinWait()}, de propósito, para o consumo de CPU ser real.
 */
public final class CpuBurnServiceExecutor implements ServiceExecutor {

    @Override
    public void serve(Duration size) throws InterruptedException {
        long deadline = System.nanoTime() + size.toNanos();
        while (deadline - System.nanoTime() > 0) {
            if (Thread.interrupted()) {
                throw new InterruptedException("atendimento interrompido");
            }
        }
    }
}
