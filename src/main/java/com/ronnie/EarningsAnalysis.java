package com.ronnie;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import org.apache.commons.math3.distribution.TDistribution;
/**
 * Analyzes earnings forecast accuracy for large-cap and small-cap firms
 * from 2015 to 2025.
 *
 * <p>The class loads stock prices and earnings data, calculates forecast
 * errors, summarizes results, runs statistical tests, applies 1%
 * winsorization, and exports data.</p>
 *
 * <p><b>Absolute forecast error:</b></p>
 * <pre>
 * |Actual EPS - Forecast EPS| / Previous Closing Price × 100
 * </pre>
 *
 * <p>Lower values mean more accurate forecasts.</p>
 *
 * <p><b>Signed forecast error:</b></p>
 * <pre>
 * (Actual EPS - Forecast EPS) / Previous Closing Price × 100
 * </pre>
 *
 * <p>Positive values mean actual EPS was above the forecast.
 * Negative values mean actual EPS was below the forecast.</p>
 *
 * <p>Study periods:</p>
 * <ul>
 *   <li>2015-2019: pre-pandemic</li>
 *   <li>2020-2022: pandemic</li>
 *   <li>2023-2025: AI era</li>
 * </ul>
 */
public class EarningsAnalysis {
    /** Stores loaded stock objects by ticker symbol. */
    static ConcurrentHashMap<String, Stock> stockMap =new ConcurrentHashMap<>();
    /** MySQL server host name or IP address. */
    static String dburl="192.168.1.129";
    /** MySQL user name. */
    static String dbusr="smart";
    /** MySQL password. */
    static String dbpwd="smart";
    /** MySQL server port. */
    static String dbport="3306";
    /** Database containing Benzinga earnings records. */
    static String earningDB="stock_endofday_profile";
    /** Database containing historical daily stock prices. */
    static String stockDB="current_endofday_price";
    //static String symbolList="C:/smart/ronnie/data/symbols.txt";
    /** File containing the ticker symbols included in the current sample. */
    static String symbolList="C:/smart/ronnie/data/spy2025.txt";
    /**
     * Program entry point.
     * Loads stock price history and earnings data,
     * then runs the selected analysis routine.
     */
    public static void main(String[] args) {
        loadAllStockData(stockDB,symbolList,20150101,20251231);
        readEarnData(earningDB,20150101,20251231);
        //calcSample();// Table 1
        //calcPriceScaledAbsoluteStat();// Tables 2A, 2B, and 3; Figure 1; Appendices A and B
        //calcSignedStat(); // Table 5A
        //testSpyAndIwmTrim();// Table 4A
        //testSpyAndIwmTrimSigned();// Table 5B
        exportSpyAndIwmWinsorized();// Table 4B
    }
    /**
     * Performs a one-sample t-test to determine whether
     * the mean forecast bias is statistically different from zero.
     *
     * @param name descriptive label for the sample
     * @param data signed forecast errors
     */
    public static void oneSampleTTestVsZero(String name,List<Double> data) {

        int n = data.size();
        if (n < 2) {
            System.out.println("Not enough data");
            return;
        }

        /*
         * Step 1: Calculate the mean.
         *
         *     mean = Σx / n
         *
         * For signed errors, the mean shows average forecast bias.
         */
        double sum = 0.0;
        for (double x : data) {
            sum += x;
        }
        double mean = sum / n;

        /*
         * Step 2: Calculate sample variance and standard deviation.
         *
         *     variance = Σ(x - mean)^2 / (n - 1)
         *     standard deviation = sqrt(variance)
         */
        double sqSum = 0.0;
        for (double x : data) {
            sqSum += (x - mean) * (x - mean);
        }
        double variance = sqSum / (n - 1);
        double std = Math.sqrt(variance);

        /*
         * Step 3: Calculate the standard error.
         *
         *     standard error = standard deviation / sqrt(n)
         */
        double se = std / Math.sqrt(n);

        /*
         * Step 4: Calculate the t-statistic.
         *
         * The test value is zero, so:
         *
         *     t = mean / standard error
         */
        double tStat = mean / se;

        /*
         * Step 5: Calculate the two-sided p-value
         * using a t-distribution with n - 1 degrees of freedom.
         */
        TDistribution tDist = new TDistribution(n - 1);
        double pValue = 2.0 * (1.0 - tDist.cumulativeProbability(Math.abs(tStat)));

        // Print results
        System.out.println(name);
        System.out.println("N = " + n);
        System.out.println("Mean = " + mean);
        System.out.println("Std = " + std);
        System.out.println("t-stat (vs 0) = " + tStat);
        System.out.println("p-value = " + pValue);
    }

    /**
     * Converts a calendar year into one of the three study periods:
     * 2015-2019 (pre-pandemic),
     * 2020-2022 (pandemic),
     * 2023-2025 (post-2022 / AI era).
     */
    private static String getPeriod(int year) {
        if (year <= 2019) return "2015-2019";
        if (year <= 2022) return "2020-2022";
        return "2023-2025";
    }
    /**
     * Calculates sample characteristics including:
     * number of firms, number of earnings announcements,
     * and average stock price for each period.
     */
    public static void calcSample() {
        Map<String, Integer> firmsCount = new HashMap<>();
        Map<String, Integer> earningsCount = new HashMap<>();
        Map<String, List<Double>> priceSum = new HashMap<>();

        for (Stock stock : stockMap.values()) {
            Map<String, Integer> record = new HashMap<>();
            for(int i=1;i<stock.qtickArray.size();i++) {
                if(stock.get(i-1).close<1) continue;
                int year=stock.get(i).date/10000;
                String period = getPeriod(year);
                priceSum.computeIfAbsent(period, y -> new ArrayList<>()).add(stock.get(i).close);
                priceSum.computeIfAbsent("2015-2025", y -> new ArrayList<>()).add(stock.get(i).close);
                //
                record.put(period,1);
                record.put("2015-2025",1);
                //
                double estimateEPS=stock.get(i).estimateEPS;
                double actualEPS =stock.get(i).actualEPS;
                if(estimateEPS==0&& actualEPS ==0) continue;
                if (!earningsCount.containsKey(period)) {
                    earningsCount.put(period, 0);
                }
                if (!earningsCount.containsKey("2015-2025")) {
                    earningsCount.put("2015-2025", 0);
                }
                earningsCount.put(period, earningsCount.get(period)+ 1);
                earningsCount.put("2015-2025", earningsCount.get("2015-2025")+ 1);
            }
            for (Map.Entry<String, Integer> entry : record.entrySet()) {
                String key = entry.getKey();
                Integer value = entry.getValue();
                if (!firmsCount.containsKey(key)) {
                    firmsCount.put(key, 0);
                }
                firmsCount.put(key,firmsCount.get(key)+1);
            }
        }
        for (Map.Entry<String, Integer> entry : firmsCount.entrySet()) {
            String key = entry.getKey();
            Integer  firms= entry.getValue();
            Integer earnings=earningsCount.get(key);
            List<Double> prices = priceSum.get(key);
            double mean = prices.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            System.out.println(key+" firms:"+firms+" earnings:"+earnings+" avg Price:"+mean);
        }
        //

    }
    /**
     * Computes price-scaled absolute forecast errors:
     * |Actual EPS - Forecast EPS| / Price.
     */
    public static void calcPriceScaledAbsoluteStat() {
        PrintOut.setFile("C:/smart/ronnie/data/earningStat.txt",true);
        PrintOut.println("**************************************************************");
        PrintOut.println(symbolList);
        Map<Integer, List<Double>> yearlyErrors = new HashMap<>();
        for (Stock stock : stockMap.values()) {
            for(int i=1;i<stock.qtickArray.size();i++) {
                double estimateEPS=stock.get(i).estimateEPS;
                double actualEPS =stock.get(i).actualEPS;
                if(estimateEPS==0&& actualEPS ==0) continue;
                double avgPrice=stock.get(i-1).close;//stock.calcAvgPrice(i-1,66);
                if(avgPrice<1) continue;
                /*
                 * Absolute forecast error scaled by the previous closing price:
                 *
                 *     |Actual EPS - Forecast EPS| / Price × 100
                 *
                 * Scaling makes errors easier to compare across firms.
                 */
                double error=Math.abs(actualEPS-estimateEPS)/avgPrice*100;
                int year=stock.get(i).date/10000;
                System.out.println(stock.get(i).date+" "+avgPrice+" "+Math.abs(actualEPS-estimateEPS));
                yearlyErrors.computeIfAbsent(year, y -> new ArrayList<>()).add(error);
            }
        }
        EarningsAnalysis.analyze(yearlyErrors);
    }
    //
    /**
     * Computes signed forecast errors:
     * (Actual EPS - Forecast EPS) / Price.
     */
    public static void calcSignedStat() {
        PrintOut.setFile("C:/smart/ronnie/data/earningStat.txt",true);
        PrintOut.println("**************************************************************");
        PrintOut.println(symbolList);
        Map<Integer, List<Double>> yearlyErrors = new HashMap<>();
        for (Stock stock : stockMap.values()) {
            for(int i=1;i<stock.qtickArray.size();i++) {
                double estimateEPS=stock.get(i).estimateEPS;
                double actualEPS =stock.get(i).actualEPS;
                if(estimateEPS==0&& actualEPS ==0) continue;
                double avgPrice=stock.get(i-1).close;//stock.calcAvgPrice(i-1,66);
                if(avgPrice<1) continue;
                /*
                 * Signed forecast error scaled by the previous closing price:
                 *
                 *     (Actual EPS - Forecast EPS) / Price × 100
                 *
                 * Positive means actual EPS was above the forecast.
                 * Negative means it was below the forecast.
                 */
                double error=(actualEPS-estimateEPS)/avgPrice*100;
                int year=stock.get(i).date/10000;
                System.out.println(stock.get(i).date+" "+avgPrice+" "+Math.abs(actualEPS-estimateEPS));
                yearlyErrors.computeIfAbsent(year, y -> new ArrayList<>()).add(error);
            }
        }
        EarningsAnalysis.analyzeSigned(yearlyErrors);
    }
    //
    /**
     * Compares raw price-scaled absolute forecast errors between
     * the S&P 500 sample and the Russell 2000 sample for each period.
     */
    public static void testSpyAndIwm() {
        PrintOut.setFile("C:/smart/ronnie/data/earningStat.txt",true);
        PrintOut.println("**************************************************************");
        PrintOut.println(symbolList);
        //
        ConcurrentHashMap<String, Stock> stockMap0 =stockMap;
        //
        symbolList="C:/smart/ronnie/data/iwm2025.txt";
        stockMap =new ConcurrentHashMap<>();
        loadAllStockData(stockDB,symbolList,20150101,20251231);
        readEarnData(earningDB,20150101,20251231);
        //
        //Map<Integer, List<Double>> yearlyErrors = new HashMap<>();
        Map<String, List<Double>> spyErrors = new TreeMap<>();
        Map<String, List<Double>> iwmErrors = new TreeMap<>();
        for (Stock stock : stockMap0.values()) {
            for(int i=1;i<stock.qtickArray.size();i++) {
                double estimateEPS=stock.get(i).estimateEPS;
                double actualEPS =stock.get(i).actualEPS;
                if(estimateEPS==0&& actualEPS ==0) continue;
                double avgPrice=stock.get(i-1).close;//stock.calcAvgPrice(i-1,66);
                //if(avgPrice<1) continue;
                double error=Math.abs(actualEPS-estimateEPS)/avgPrice*100;
                int year=stock.get(i).date/10000;
                String period = getPeriod(year);
                spyErrors.computeIfAbsent(period, y -> new ArrayList<>()).add(error);
            }
        }
        for (Stock stock : stockMap.values()) {
            for(int i=1;i<stock.qtickArray.size();i++) {
                double estimateEPS=stock.get(i).estimateEPS;
                double actualEPS =stock.get(i).actualEPS;
                if(estimateEPS==0&& actualEPS ==0) continue;
                double avgPrice=stock.get(i-1).close;//stock.calcAvgPrice(i-1,66);
                if(avgPrice<1) continue;
                double error=Math.abs(actualEPS-estimateEPS)/avgPrice*100;
                int year=stock.get(i).date/10000;
                String period = getPeriod(year);
                iwmErrors.computeIfAbsent(period, y -> new ArrayList<>()).add(error);
            }
        }
        PrintOut.println("\n=== PAIRWISE T-TESTS (Welch) ===");

        runTest("2015-2019",
                spyErrors.get("2015-2019"),
                iwmErrors.get("2015-2019"));

        runTest("2020-2022",
                spyErrors.get("2020-2022"),
                iwmErrors.get("2020-2022"));

        runTest("2023-2025",
                spyErrors.get("2023-2025"),
                iwmErrors.get("2023-2025"));
    }
    //
    //
    /**
     * Compares 1% winsorized absolute forecast errors between
     * the S&P 500 and Russell 2000 samples.
     */
    public static void testSpyAndIwmTrim() {
        PrintOut.setFile("C:/smart/ronnie/data/earningStat.txt",true);
        PrintOut.println("**************************************************************");
        PrintOut.println(symbolList);
        //
        ConcurrentHashMap<String, Stock> stockMap0 =stockMap;
        //
        symbolList="C:/smart/ronnie/data/iwm2025.txt";
        stockMap =new ConcurrentHashMap<>();
        loadAllStockData(stockDB,symbolList,20150101,20251231);
        readEarnData(earningDB,20150101,20251231);
        //
        //Map<Integer, List<Double>> yearlyErrors = new HashMap<>();
        Map<String, List<Double>> spyErrors = new TreeMap<>();
        Map<String, List<Double>> iwmErrors = new TreeMap<>();
        for (Stock stock : stockMap0.values()) {
            for(int i=1;i<stock.qtickArray.size();i++) {
                double estimateEPS=stock.get(i).estimateEPS;
                double actualEPS =stock.get(i).actualEPS;
                if(estimateEPS==0&& actualEPS ==0) continue;
                double avgPrice=stock.get(i-1).close;//stock.calcAvgPrice(i-1,66);
                //if(avgPrice<1) continue;
                double error=Math.abs(actualEPS-estimateEPS)/avgPrice*100;
                int year=stock.get(i).date/10000;
                String period = getPeriod(year);
                spyErrors.computeIfAbsent(period, y -> new ArrayList<>()).add(error);
            }
        }
        for (Stock stock : stockMap.values()) {
            for(int i=1;i<stock.qtickArray.size();i++) {
                double estimateEPS=stock.get(i).estimateEPS;
                double actualEPS =stock.get(i).actualEPS;
                if(estimateEPS==0&& actualEPS ==0) continue;
                double avgPrice=stock.get(i-1).close;//stock.calcAvgPrice(i-1,66);
                if(avgPrice<1) continue;
                double error=Math.abs(actualEPS-estimateEPS)/avgPrice*100;
                int year=stock.get(i).date/10000;
                String period = getPeriod(year);
                iwmErrors.computeIfAbsent(period, y -> new ArrayList<>()).add(error);
            }
        }
        PrintOut.println("\n=== PAIRWISE T-TESTS (Welch) ===");

        runTestTrim("2015-2019",
                spyErrors.get("2015-2019"),
                iwmErrors.get("2015-2019"));

        runTestTrim("2020-2022",
                spyErrors.get("2020-2022"),
                iwmErrors.get("2020-2022"));

        runTestTrim("2023-2025",
                spyErrors.get("2023-2025"),
                iwmErrors.get("2023-2025"));
    }
    //
    /**
     * Compares 1% winsorized signed forecast errors between
     * the S&P 500 and Russell 2000 samples.
     */
    public static void testSpyAndIwmTrimSigned() {
        PrintOut.setFile("C:/smart/ronnie/data/earningStat.txt",true);
        PrintOut.println("**************************************************************");
        PrintOut.println(symbolList);
        //
        ConcurrentHashMap<String, Stock> stockMap0 =stockMap;
        //
        symbolList="C:/smart/ronnie/data/iwm2025.txt";
        stockMap =new ConcurrentHashMap<>();
        loadAllStockData(stockDB,symbolList,20150101,20251231);
        readEarnData(earningDB,20150101,20251231);
        //
        //Map<Integer, List<Double>> yearlyErrors = new HashMap<>();
        Map<String, List<Double>> spyErrors = new TreeMap<>();
        Map<String, List<Double>> iwmErrors = new TreeMap<>();
        for (Stock stock : stockMap0.values()) {
            for(int i=1;i<stock.qtickArray.size();i++) {
                double estimateEPS=stock.get(i).estimateEPS;
                double actualEPS =stock.get(i).actualEPS;
                if(estimateEPS==0&& actualEPS ==0) continue;
                double avgPrice=stock.get(i-1).close;//stock.calcAvgPrice(i-1,66);
                //if(avgPrice<1) continue;
                double error=(actualEPS-estimateEPS)/avgPrice*100;
                int year=stock.get(i).date/10000;
                String period = getPeriod(year);
                spyErrors.computeIfAbsent(period, y -> new ArrayList<>()).add(error);
                spyErrors.computeIfAbsent("2025-all", y -> new ArrayList<>()).add(error);
            }
        }
        for (Stock stock : stockMap.values()) {
            for(int i=1;i<stock.qtickArray.size();i++) {
                double estimateEPS=stock.get(i).estimateEPS;
                double actualEPS =stock.get(i).actualEPS;
                if(estimateEPS==0&& actualEPS ==0) continue;
                double avgPrice=stock.get(i-1).close;//stock.calcAvgPrice(i-1,66);
                if(avgPrice<1) continue;
                double error=(actualEPS-estimateEPS)/avgPrice*100;
                int year=stock.get(i).date/10000;
                String period = getPeriod(year);
                iwmErrors.computeIfAbsent(period, y -> new ArrayList<>()).add(error);
                iwmErrors.computeIfAbsent("2025-all", y -> new ArrayList<>()).add(error);
            }
        }
        PrintOut.println("\n=== PAIRWISE T-TESTS (Welch) ===");

        runTestTrim("2015-2019",
                spyErrors.get("2015-2019"),
                iwmErrors.get("2015-2019"));

        runTestTrim("2020-2022",
                spyErrors.get("2020-2022"),
                iwmErrors.get("2020-2022"));

        runTestTrim("2023-2025",
                spyErrors.get("2023-2025"),
                iwmErrors.get("2023-2025"));
        runTestTrim("2025-all",
                spyErrors.get("2025-all"),
                iwmErrors.get("2025-all"));
    }
    //
    /**
     * Creates 1% winsorized period-level error samples for both indices
     * and exports them to CSV files for boxplot construction.
     */
    public static void exportSpyAndIwmWinsorized() {
        PrintOut.setFile("C:/smart/ronnie/data/earningStat.txt",true);
        //PrintOut.println("**************************************************************");
        //PrintOut.println(symbolList);
        //
        ConcurrentHashMap<String, Stock> stockMap0 =stockMap;
        //
        symbolList="C:/smart/ronnie/data/iwm2025.txt";
        stockMap =new ConcurrentHashMap<>();
        loadAllStockData(stockDB,symbolList,20150101,20251231);
        readEarnData(earningDB,20150101,20251231);
        //
        //Map<Integer, List<Double>> yearlyErrors = new HashMap<>();
        Map<String, List<Double>> spyErrors = new TreeMap<>();
        Map<String, List<Double>> iwmErrors = new TreeMap<>();
        for (Stock stock : stockMap0.values()) {
            for(int i=1;i<stock.qtickArray.size();i++) {
                double estimateEPS=stock.get(i).estimateEPS;
                double actualEPS =stock.get(i).actualEPS;
                if(estimateEPS==0&& actualEPS ==0) continue;
                double avgPrice=stock.get(i-1).close;//stock.calcAvgPrice(i-1,66);
                //if(avgPrice<1) continue;
                double error=Math.abs(actualEPS-estimateEPS)/avgPrice*100;
                int year=stock.get(i).date/10000;
                String period = getPeriod(year);
                spyErrors.computeIfAbsent(period, y -> new ArrayList<>()).add(error);
                spyErrors.computeIfAbsent("2025-all", y -> new ArrayList<>()).add(error);
            }
        }
        for (Stock stock : stockMap.values()) {
            for(int i=1;i<stock.qtickArray.size();i++) {
                double estimateEPS=stock.get(i).estimateEPS;
                double actualEPS =stock.get(i).actualEPS;
                if(estimateEPS==0&& actualEPS ==0) continue;
                double avgPrice=stock.get(i-1).close;//stock.calcAvgPrice(i-1,66);
                if(avgPrice<1) continue;
                double error=Math.abs(actualEPS-estimateEPS)/avgPrice*100;
                int year=stock.get(i).date/10000;
                String period = getPeriod(year);
                iwmErrors.computeIfAbsent(period, y -> new ArrayList<>()).add(error);
                iwmErrors.computeIfAbsent("2025-all", y -> new ArrayList<>()).add(error);
            }
        }
        Map<String, List<Double>> spyErrorsW =Winsorizer.winsorizeMap(spyErrors,0.01);
        Map<String, List<Double>> iwmErrorsW =Winsorizer.winsorizeMap(iwmErrors,0.01);
        try {
            writeBoxplotCSV(spyErrorsW,"c:/smart/ronnie/data/spyerrorw.csv");
            writeBoxplotCSV(iwmErrorsW,"c:/smart/ronnie/data/iwmerrorw.csv");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    //
    /**
     * Writes period data to a column-based CSV file.
     * Each period becomes one column. Shorter columns use blank cells.
     *
     * @param data period names and values
     * @param fileName output CSV file
     * @throws IOException if the file cannot be written
     */
    public static void writeBoxplotCSV(Map<String, List<Double>> data,
                                       String fileName) throws IOException {

        try (PrintWriter out = new PrintWriter(new FileWriter(fileName))) {

            // Write header
            boolean first = true;
            for (String key : data.keySet()) {
                if (!first) out.print(",");
                out.print(key);
                first = false;
            }
            out.println();

            // Find the longest column
            int maxRows = 0;
            for (List<Double> list : data.values()) {
                maxRows = Math.max(maxRows, list.size());
            }

            // Write data rows
            for (int row = 0; row < maxRows; row++) {

                first = true;

                for (List<Double> list : data.values()) {

                    if (!first) out.print(",");

                    if (row < list.size()) {
                        out.print(list.get(row));
                    }

                    first = false;
                }

                out.println();
            }
        }
    }
    // -----------------------------
    // MAIN ANALYSIS METHOD
    // -----------------------------
    /**
     * Produces yearly statistics, period statistics,
     * and Welch t-tests for forecast accuracy.
     */
    public static void analyze(Map<Integer, List<Double>> yearlyErrors) {

        Map<Integer, ResearchStats> yearlyStats = new TreeMap<>();
        Map<String, ResearchStats> periodStats = new TreeMap<>();
        Map<String, List<Double>> periodErrors = new TreeMap<>();

        // =============================
        // 1. BUILD YEARLY + PERIOD DATA
        // =============================
        for (Map.Entry<Integer, List<Double>> entry : yearlyErrors.entrySet()) {

            int year = entry.getKey();
            String period = getPeriod(year);

            for (double error : entry.getValue()) {
                /*
                 * Add each error to its year, study period,
                 * and the full 2015-2025 sample.
                 */
                yearlyStats.computeIfAbsent(year, y -> new ResearchStats()).add(error);
                periodStats.computeIfAbsent(period, p -> new ResearchStats()).add(error);
                periodErrors.computeIfAbsent(period, p -> new ArrayList<>()).add(error);
                //
                yearlyStats.computeIfAbsent(2030, y -> new ResearchStats()).add(error);
                periodStats.computeIfAbsent("2025-all", p -> new ResearchStats()).add(error);
                periodErrors.computeIfAbsent("2025-all", p -> new ArrayList<>()).add(error);
            }
        }

        // =============================
        // 2. PRINT YEARLY STATISTICS
        // =============================
        System.out.println("\n=== YEARLY STATISTICS ===");
        System.out.println("Year,N,Mean,Std,MAE,RMSE,Median,P25,P75,Winsor");
        PrintOut.println("\n=== YEARLY STATISTICS ===");
        PrintOut.println("Year,N,Mean,Std,MAE,RMSE,Median,P25,P75,Winsor");
        for (Map.Entry<Integer, ResearchStats> e : yearlyStats.entrySet()) {
            int year = e.getKey();
            ResearchStats s = e.getValue();
            String line=year + "," +
                            s.count() + "," +
                            s.mean() + "," +
                            s.std() + "," +
                            s.mae() + "," +
                            s.rmse() + "," +
                            s.median() + "," +
                            s.p25() + "," +
                            s.p75() + "," +
                            s.winsorizedMean(0.01);
            System.out.println(line);
            PrintOut.println(line);
        }

        // =============================
        // 3. PRINT PERIOD STATISTICS
        // =============================
        System.out.println("\n=== PERIOD STATISTICS ===");
        System.out.println("Year,N,Mean,Std,MAE,RMSE,Median,P25,P75,Winsor");
        PrintOut.println("\n=== PERIOD STATISTICS ===");
        PrintOut.println("Year,N,Mean,Std,MAE,RMSE,Median,P25,P75,Winsor");
        //
        for (Map.Entry<String, ResearchStats> e : periodStats.entrySet()) {
            String p = e.getKey();
            ResearchStats s = e.getValue();

            String line=p + "," +
                            s.count() + "," +
                            s.mean() + "," +
                            s.std() + "," +
                            s.mae() + "," +
                            s.rmse() + "," +
                            s.median() + "," +
                            s.p25() + "," +
                            s.p75() + "," +
                            s.winsorizedMean(0.01);
            System.out.println(line);
            PrintOut.println(line);
        }

        // =============================
        // 4. T-TEST + P-VALUE
        // =============================
        PrintOut.println("\n=== PAIRWISE T-TESTS (Welch) ===");

        runTest("2015-2019 vs 2020-2022",
                periodErrors.get("2015-2019"),
                periodErrors.get("2020-2022"));

        runTest("2020-2022 vs 2023-2025",
                periodErrors.get("2020-2022"),
                periodErrors.get("2023-2025"));

        runTest("2015-2019 vs 2023-2025",
                periodErrors.get("2015-2019"),
                periodErrors.get("2023-2025"));
    }
    //
    /**
     * Produces descriptive statistics and one-sample t-tests
     * for signed forecast errors.
     */
    public static void analyzeSigned(Map<Integer, List<Double>> yearlyErrors) {

        Map<Integer, ResearchStats> yearlyStats = new TreeMap<>();
        Map<String, ResearchStats> periodStats = new TreeMap<>();
        Map<String, List<Double>> periodErrors = new TreeMap<>();
        Map<String, List<Double>> temp = new TreeMap<>();
        //
        for (Map.Entry<Integer, List<Double>> entry : yearlyErrors.entrySet()) {

            int year = entry.getKey();
            String period = getPeriod(year);
            //
            for (double error : entry.getValue()) {
                //yearlyStats.computeIfAbsent(year, y -> new ResearchStats()).add(error);
                //periodStats.computeIfAbsent(period, p -> new ResearchStats()).add(error);
                temp.computeIfAbsent(period, p -> new ArrayList<>()).add(error);
                //
                //yearlyStats.computeIfAbsent(2030, y -> new ResearchStats()).add(error);
                //periodStats.computeIfAbsent("2025-all", p -> new ResearchStats()).add(error);
                temp.computeIfAbsent("2025-all", p -> new ArrayList<>()).add(error);
            }
        }
        for (Map.Entry<String, List<Double>> entry : temp.entrySet()) {
            String key=entry.getKey();
            List<Double> a0=entry.getValue();
            Collections.sort(a0);

            /*
             * Apply 1% winsorization.
             * Replace the lowest and highest 1% with cutoff values
             * to reduce the effect of extreme errors.
             */
            int n = a0.size();
            int k = (int) Math.floor(0.01 * n);
            //
            for (int i = 0; i < n; i++) {
                double v = a0.get(i);
                if (i < k) v = a0.get(k);
                if (i >= n - k) v = a0.get(n - k - 1);
                periodStats.computeIfAbsent(key, p -> new ResearchStats()).add(v);
                periodErrors.computeIfAbsent(key, p -> new ArrayList<>()).add(v);
            }
        }
        // =============================


        // =============================
        // 3. PRINT PERIOD STATISTICS
        // =============================
        System.out.println("\n=== PERIOD STATISTICS ===");
        System.out.println("Year,N,Mean,Std,MAE,RMSE,Median,P25,P75,Winsor");
        PrintOut.println("\n=== PERIOD STATISTICS ===");
        PrintOut.println("Year,N,Mean,Std,MAE,RMSE,Median,P25,P75,Winsor");
        //
        for (Map.Entry<String, ResearchStats> e : periodStats.entrySet()) {
            String p = e.getKey();
            ResearchStats s = e.getValue();

            String line=p + "," +
                    s.count() + "," +
                    s.mean() + "," +
                    s.std() + "," +
                    s.mae() + "," +
                    s.rmse() + "," +
                    s.median() + "," +
                    s.p25() + "," +
                    s.p75() + "," +
                    s.winsorizedMean(0.01);
            System.out.println(line);
            PrintOut.println(line);
        }

        // =============================
        // 4. T-TEST + P-VALUE
        // =============================
        PrintOut.println("\n=== PAIRWISE T-TESTS (Welch) ===");

        oneSampleTTestVsZero("2015-2019",periodErrors.get("2015-2019"));
        oneSampleTTestVsZero("2020-2022",periodErrors.get("2020-2022"));
        oneSampleTTestVsZero("2023-2025",periodErrors.get("2023-2025"));
        oneSampleTTestVsZero("2025-all",periodErrors.get("2025-all"));
    }

    //
    /**
     * Creates a MySQL database connection.
     */
    static Connection getMySqlConnection(String db) {
        try
        {
            Class.forName("com.mysql.cj.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            System.out.println("MySQL JDBC Driver not found !!");
            return null;
        }
        System.out.println("MySQL JDBC Driver Registered!");
        Connection connection = null;
        try {
            String url="jdbc:mysql://"+dburl+":"+dbport+"/"+db;
            connection = DriverManager.getConnection(url, dbusr, dbpwd);
            System.out.println("SQL Connection to database established!");
        } catch (Exception e) {
            System.out.println("Connection Failed! Check output console");
            return null;
        }
        return connection;
    }
    /**
     * Reads Benzinga earnings records and merges them
     * into the corresponding stock price series.
     */
    static void readEarnData(String db,int beginDate,int endDate) {
        //
        Connection connection=getMySqlConnection(db);
        //
        int tot=0;
        try{
            assert connection != null;
            Statement stmt=connection.createStatement();
            ResultSet rs=stmt.executeQuery("select * from Benzinga_earning where date>="+beginDate+" and date<="+endDate);
            while(rs.next()) {

                //
                int date=rs.getInt(1);
                int time=rs.getInt(2);
                String symbol=rs.getString(3);
                double actual_eps=rs.getDouble(4);
                double estimate_eps=rs.getDouble(5);
                double  previous_eps=rs.getDouble(6);
                int importance=rs.getInt(7);
                //
                Stock c=stockMap.get(symbol);
                if(c==null) continue;
                int id=c.findQtickID(date);
                if(id<0) continue;
                c.get(id).estimateEPS=estimate_eps;
                c.get(id).actualEPS=actual_eps;
                c.get(id).previousEPS=previous_eps;
                c.get(id).releaseTime=time;
                c.get(id).importance=importance;
                tot++;
            }
            rs.close();
            stmt.close();
        }
        catch(Exception e){
            System.out.println(e);
        }
        //}
        //
        try {
            if(connection != null) connection.close();
            System.out.println("Connection closed !!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("total earning record:"+tot);
    }
    // -----------------------------
    // T-TEST (Welch) + P-VALUE
    // -----------------------------
    /**
     * Runs a Welch t-test to compare two groups.
     * The groups may have different sizes and variances.
     */
    private static void runTest(String name, List<Double> a, List<Double> b) {

        double meanA = mean(a);
        double meanB = mean(b);

        double varA = variance(a, meanA);
        double varB = variance(b, meanB);

        /*
         * Welch t-statistic:
         *
         *     t = (meanA - meanB)
         *         / sqrt(varA / nA + varB / nB)
         */
        double t = (meanA - meanB) /
                Math.sqrt(varA / a.size() + varB / b.size());

        /*
         * Use min(nA, nB) - 1 degrees of freedom.
         * This is a simplified version of the standard Welch test.
         */
        int df = Math.min(a.size(), b.size()) - 1;

        TDistribution dist = new TDistribution(df);
        double p = 2 * (1 - dist.cumulativeProbability(Math.abs(t)));

        System.out.println(name);
        System.out.println("t-stat = " + t);
        System.out.println("p-value = " + p);
        System.out.println("---------------------");
        //
        PrintOut.println(name);
        PrintOut.println("t-stat = " + t);
        PrintOut.println("p-value = " + p);
        PrintOut.println("---------------------");
    }
    private static void runTestTrim(String name, List<Double> a0, List<Double> b0) {
        //
        Collections.sort(a0);
        List<Double> a=new ArrayList<>();
        int n = a0.size();
        int k = (int) Math.floor(0.01 * n);
        //
        for (int i = 0; i < n; i++) {
            double v = a0.get(i);
            if (i < k) v = a0.get(k);
            if (i >= n - k) v = a0.get(n - k - 1);
            a.add(v);
        }
        //
        Collections.sort(b0);
        List<Double> b=new ArrayList<>();
        n = b0.size();
        k = (int) Math.floor(0.01 * n);
        //
        for (int i = 0; i < n; i++) {
            double v = b0.get(i);
            if (i < k) v = b0.get(k);
            if (i >= n - k) v = b0.get(n - k - 1);
            b.add(v);
        }
        //
        double meanA = mean(a);
        double meanB = mean(b);

        double varA = variance(a, meanA);
        double varB = variance(b, meanB);

        double t = (meanA - meanB) /
                Math.sqrt(varA / a.size() + varB / b.size());

        int df = Math.min(a.size(), b.size()) - 1;

        TDistribution dist = new TDistribution(df);
        double p = 2 * (1 - dist.cumulativeProbability(Math.abs(t)));

        System.out.println(name);
        System.out.println("mean diff="+(meanA-meanB));
        System.out.println("t-stat = " + t);
        System.out.println("p-value = " + p);
        System.out.println("---------------------");
        //
        PrintOut.println(name);
        PrintOut.println("mean diff="+(meanA-meanB));
        PrintOut.println("t-stat = " + t);
        PrintOut.println("p-value = " + p);
        PrintOut.println("---------------------");
    }

    // -----------------------------
    // HELPERS
    // -----------------------------
    /**
     * Calculates the mean.
     *
     * @param x values
     * @return mean, or zero if the list is empty
     */
    private static double mean(List<Double> x) {
        return x.stream().mapToDouble(d -> d).average().orElse(0.0);
    }

    /**
     * Calculates sample variance.
     *
     * @param x values
     * @param mean mean of the values
     * @return sample variance
     */
    private static double variance(List<Double> x, double mean) {
        double sum = 0.0;
        for (double v : x) sum += (v - mean) * (v - mean);
        return sum / (x.size() - 1);
    }
    /**
     * Loads historical daily stock prices for all symbols
     * listed in the input symbol file.
     */
    static void loadAllStockData(String db,String f,int fromDate,int toDate)  {
        File file = new File(f);
        if(!file.exists()) {
            System.out.println(f+" does not exist");
            System.exit(0);
        }
        Connection stdb=getMySqlConnection(db);
        if(stdb==null) {
            System.out.println("can not connect to stock_endofday_price");
            System.exit(0);
        }
        // Open the symbol file
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            System.out.print("file not found" + f);
        }
        //

        int tot=0;
        String line;
        try {
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if(parts.length == 0) continue;
                String symbol=parts[0];
                symbol=symbol.trim().toUpperCase();
                if(symbol.isEmpty()) continue;
                if(symbol.charAt(0)=='/'||symbol.charAt(0)=='#') {
                    continue;
                }
                Stock stock=new Stock();
                stock.readStockDailyDB(stdb,symbol,fromDate,toDate);
                stock.symbol=symbol;
                stockMap.put(symbol,stock);
                System.out.println(tot+" "+symbol+":"+stock.qtickArray.size());
                tot++;
            }
            System.out.println("total stock read:"+tot);
            br.close();
            stdb.close();
        }
        catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }
}