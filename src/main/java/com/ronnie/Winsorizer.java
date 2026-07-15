package com.ronnie;

import java.util.*;

public class Winsorizer {

    /**
     * Winsorize every List<Double> in a TreeMap.
     *
     * @param original Original TreeMap
     * @param percentile Percentile to winsorize (e.g. 0.01 = 1%)
     * @return New TreeMap with winsorized values
     */
    public static TreeMap<String, List<Double>> winsorizeMap(
            Map<String, List<Double>> original,
            double percentile) {

        TreeMap<String, List<Double>> result = new TreeMap<>();

        for (Map.Entry<String, List<Double>> entry : original.entrySet()) {
            result.put(entry.getKey(),
                    winsorizeList(entry.getValue(), percentile));
        }

        return result;
    }

    /**
     * Winsorize one list.
     */
    public static List<Double> winsorizeList(List<Double> values,
                                             double percentile) {

        if (values == null || values.isEmpty()) {
            return new ArrayList<>();
        }

        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);

        int n = sorted.size();

        int lowerIndex = (int) Math.floor(percentile * n);
        int upperIndex = (int) Math.ceil((1.0 - percentile) * n) - 1;

        lowerIndex = Math.max(0, lowerIndex);
        upperIndex = Math.min(n - 1, upperIndex);

        double lower = sorted.get(lowerIndex);
        double upper = sorted.get(upperIndex);

        List<Double> result = new ArrayList<>(n);

        for (double v : values) {
            if (v < lower) {
                result.add(lower);
            } else if (v > upper) {
                result.add(upper);
            } else {
                result.add(v);
            }
        }

        return result;
    }
}