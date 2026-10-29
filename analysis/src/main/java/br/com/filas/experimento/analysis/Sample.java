package br.com.filas.experimento.analysis;

import java.util.Arrays;

/** Amostra de uma métrica (em ms): média, segundo momento e quantis por posto mais próximo. */
record Sample(int n, double mean, double secondMoment, double thirdMoment, double p50, double p95, double p99) {

    static Sample of(double[] values) {
        if (values.length == 0) {
            return new Sample(0, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        }
        double[] v = values.clone();
        Arrays.sort(v);
        double s1 = 0;
        double s2 = 0;
        double s3 = 0;
        for (double x : v) {
            s1 += x;
            s2 += x * x;
            s3 += x * x * x;
        }
        int n = v.length;
        return new Sample(n, s1 / n, s2 / n, s3 / n, quantile(v, 0.50), quantile(v, 0.95), quantile(v, 0.99));
    }

    private static double quantile(double[] sorted, double p) {
        int k = (int) Math.ceil(p * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, k))];
    }
}
