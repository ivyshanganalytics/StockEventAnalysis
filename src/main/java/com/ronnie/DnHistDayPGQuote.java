package com.ronnie;

// Fastjson is used to parse JSON responses from Polygon API

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * This class downloads historical daily stock prices
 * from Polygon.io and saves them either to files or a database.
 *
 * - Read stock symbols
 * - Call Polygon API
 * - Save daily OHLCV data
 */
public class DnHistDayPGQuote extends Thread {

    // List to store all stock symbols
    static ArrayList<String> tickArray = new ArrayList<>();

    // Polygon API key (replace with your own)
    static String apikey = "your polygon key here";

    // Max number of records per API call
    static int maxRecord = 50000;

    // Earliest date to download (YYYYMMDD)
    static int firstDate = 20200101;

    // File that contains stock symbols (one per line)
    static String tickFile = "C:/smart/ronnie/data/symbols.txt";
    // Whether prices are adjusted or not
    static String split = "false";

    // Database connection (only used in DATABASE mode)
    static Connection stdb;
    static String dbName="stock_endofday_price";
    static String dburl="192.168.1.129";
    static String dbusr="smart";
    static String dbpwd="smart";
    static String dbport="3306";

    DnHistDayPGQuote() {}

    public static void main(String[] args) throws Exception {

        // Read stock symbols from text file
        List<String> symbols = ReadFileIntoArray(tickFile);

        //connect database first
        stdb = getMySqlConnection(dbName);
        // Download data for each stock symbol
        for (String symbol : symbols) {
            downloadDayQuote(symbol, firstDate);
        }
    }

    /**
     * Reads a text file and stores each stock symbol into a list.
     * Used for loading stock symbols.
     */
    public static ArrayList<String> ReadFileIntoArray(String file) {

        ArrayList<String> lines = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;

            // Read file line by line
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        System.out.println("reading from file: " + file +
                " total lines: " + lines.size());

        return lines;
    }

    /**
     * Converts milliseconds timestamp into YYYYMMDD format.
     */
    public static int getDate(long tt) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(tt);

        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DATE);

        return year * 10000 + (month + 1) * 100 + day;
    }

    /**
     * Returns today's date as YYYYMMDD.
     */
    public static int getDate() {
        Calendar calendar = Calendar.getInstance();

        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DATE);

        return year * 10000 + (month + 1) * 100 + day;
    }

    /**
     * Downloads daily historical prices for one stock symbol.
     */
    public static void downloadDayQuote(String symbol, int tt) {

        // Make sure date is not crazy old
        tt = Math.max(tt, 19000101);

        // HTTP client for API requests
        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .build();

        int now = getDate();

        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

        try {
            // Convert dates into API-required format
            String now1 = simpleDateFormat.format(
                    new SimpleDateFormat("yyyyMMdd").parse(Integer.toString(now)));

            String tt1 = simpleDateFormat.format(
                    new SimpleDateFormat("yyyyMMdd").parse(Integer.toString(tt)));

            // Build Polygon API URL
            String url =
                    "https://api.polygon.io/v2/aggs/ticker/" + symbol +
                            "/range/1/day/" + tt1 + "/" + now1 +
                            "?adjusted=" + split +
                            "&sort=asc&limit=" + maxRecord +
                            "&apiKey=" + apikey;

            HttpRequest request =
                    HttpRequest.newBuilder().uri(URI.create(url)).GET().build();

            System.out.println("download data for " + symbol + "....");

            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

            // Parse JSON response
            JSONObject jo = JSON.parseObject(response.body());

            // Check API status
            if (!"OK".equals(jo.get("status"))) {
                System.out.println(symbol + " REST status not OK");
                return;
            }

            // Double-check ticker symbol
            if (!symbol.equals(jo.get("ticker"))) {
                System.out.println(symbol + " ticker mismatch");
                return;
            }

            int resultsCount =
                    Double.valueOf(jo.get("resultsCount").toString()).intValue();

            // Only process if data exists
            if (resultsCount > 0) {

                JSONArray tta = (JSONArray) jo.get("results");

                System.out.println("write data for " + symbol + "....");
                writeToDatabase(symbol, tta);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Writes daily stock data into MySQL database.
     * One table per stock symbol.
     */
    static void writeToDatabase(String symbol, JSONArray tta)
            throws SQLException {

        Connection connection = stdb;

        // Create table if it doesn't exist
        try (Statement stmt = connection.createStatement()) {
            String createTable =
                    "create table if not exists " + dbName + "." + symbol +
                            " (date int not null, open float(13,4), high float(13,4), " +
                            "low float(13,4), close float(13,4), volume int, " +
                            "vwap float(13,4), primary key(date))";

            stmt.executeUpdate(createTable);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Statement stmt = connection.createStatement();
        StringBuilder sb =
                new StringBuilder("replace into " + dbName + "." + symbol + " values ");

        int tot = 0;

        // Loop through each trading day
        for (int i = 0; i < tta.size(); i++) {
            JSONObject t = (JSONObject) tta.get(i);

            try {
                long ts = (long) Double.parseDouble(t.get("t").toString());
                int date = getDate(ts);

                double open = Double.parseDouble(t.get("o").toString());
                double high = Double.parseDouble(t.get("h").toString());
                double low  = Double.parseDouble(t.get("l").toString());
                double close = Double.parseDouble(t.get("c").toString());
                long volume = Double.valueOf(t.get("v").toString()).longValue();
                double vwap = Double.parseDouble(t.get("vw").toString());

                if (tot > 0) sb.append(",");

                sb.append("(")
                        .append(date).append(",")
                        .append(open).append(",")
                        .append(high).append(",")
                        .append(low).append(",")
                        .append(close).append(",")
                        .append(volume / 100).append(",")
                        .append(vwap)
                        .append(")");

                tot++;

                // Execute in batches to avoid huge SQL statements
                if (tot > 15000) {
                    stmt.executeUpdate(sb.toString());
                    tot = 0;
                    sb = new StringBuilder(
                            "replace into " + dbName + "." + symbol + " values ");
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (tot > 0) stmt.executeUpdate(sb.toString());
        stmt.close();

        System.out.println("completed " + symbol +
                " total records: " + tta.size());
    }

    /**
     * Converts timestamp into HHMMSS time format.
     */
    public static int getTime(long tt) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(tt);

        return calendar.get(Calendar.HOUR_OF_DAY) * 10000 +
                calendar.get(Calendar.MINUTE) * 100 +
                calendar.get(Calendar.SECOND);
    }

    /**
     * Creates and returns a MySQL database connection.
     */
    public static Connection getMySqlConnection(String db) {

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("MySQL JDBC Driver not found");
            return null;
        }

        try {
            String url =
                    "jdbc:mysql://" + dburl + ":" + dbport + "/" + db;
            return DriverManager.getConnection(url, dbusr, dbpwd);
        } catch (Exception e) {
            System.out.println("Database connection failed");
            return null;
        }
    }
}
