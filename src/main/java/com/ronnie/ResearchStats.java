package com.ronnie;

import java.util.*;

/**
 * Utility class for calculating descriptive statistics for forecast errors
 * and other research-related numeric series.
 */
public class ResearchStats {

    /** Stores all observations added to the statistics object. */
    private final List<Double> values = new ArrayList<>();

    /** Adds a new observation to the sample. */
    public void add(double x) {
        values.add(x);
    }

    /** Returns the number of observations in the sample. */
    public int count() {
        return values.size();
    }

    /** Returns the arithmetic mean of the sample. */
    public double mean() {
        return values.stream().mapToDouble(d -> d).average().orElse(0.0);
    }

    /** Returns the sample variance using (n - 1) in the denominator. */
    public double variance() {
        if (values.size() < 2) return 0.0;

        double m = mean();
        double sum = 0.0;

        for (double x : values) {
            sum += (x - m) * (x - m);
        }

        return sum / (values.size() - 1);
    }

    /** Returns the sample standard deviation. */
    public double std() {
        return Math.sqrt(variance());
    }

    /** Returns the mean absolute error (MAE). */
    public double mae() {
        return values.stream().mapToDouble(Math::abs).average().orElse(0.0);
    }

    /** Returns the root mean squared error (RMSE). */
    public double rmse() {
        return Math.sqrt(values.stream()
                .mapToDouble(x -> x * x)
                .average()
                .orElse(0.0));
    }

    /** Returns the median value of the sample. */
    public double median() {
        if (values.isEmpty()) return 0.0;

        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);

        int n = sorted.size();

        if (n % 2 == 1) return sorted.get(n / 2);

        return (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
    }

    /**
     * Returns the requested percentile using a simple rank-based approach.
     *
     * @param p percentile expressed as a fraction (e.g. 0.25 for the 25th percentile)
     */
    public double percentile(double p) {
        if (values.isEmpty()) return 0.0;

        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);

        int idx = (int) Math.ceil(p * sorted.size()) - 1;
        idx = Math.max(0, Math.min(idx, sorted.size() - 1));

        return sorted.get(idx);
    }

    /** Convenience method for the 25th percentile. */
    public double p25() {
        return percentile(0.25);
    }

    /** Convenience method for the 75th percentile. */
    public double p75() {
        return percentile(0.75);
    }

    /**
     * Returns the winsorized mean after replacing extreme values
     * in both tails with boundary values.
     *
     * @param trim proportion trimmed from each tail (e.g. 0.01 for 1%)
     */
    public double winsorizedMean(double trim) {
        if (values.isEmpty()) return 0.0;

        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);

        int n = sorted.size();
        int k = (int) Math.floor(trim * n);

        double sum = 0.0;

        for (int i = 0; i < n; i++) {
            double v = sorted.get(i);

            if (i < k) v = sorted.get(k);
            if (i >= n - k) v = sorted.get(n - k - 1);

            sum += v;
        }

        return sum / n;
    }
}