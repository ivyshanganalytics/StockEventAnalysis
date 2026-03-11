package com.ronnie;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.DayOfWeek;
import java.time.LocalDate;

public class BenzingaEarning {

    static String API_KEY = "your polygon key here";

    public static void main(String[] args) {

        //
        System.out.println("java -jar BenzingaEarning.jar dashSilexx.prm");
        //System.out.println("start and end are numbers indicating stocks in symbol");
        int argc=args.length;
        //

        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        try (Connection conn = getMySqlConnection("stock_endofday_profile")) {

            conn.setAutoCommit(false);

            PreparedStatement ps = conn.prepareStatement(
                    "REPLACE INTO Benzinga_earning " +
                            "(date,time,ticker,actual_eps,estimated_eps,previous_eps,importance) " +
                            "VALUES (?,?,?,?,?,?,?)"
            );

            LocalDate start = LocalDate.of(2015, 5, 19);
            LocalDate today = LocalDate.now();

            for (LocalDate d = start; !d.isAfter(today); d = d.plusDays(1)) {
                DayOfWeek dow = d.getDayOfWeek();

                if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) {
                    continue;
                }

                String url = "https://api.polygon.io/benzinga/v1/earnings?date="
                        + d + "&limit=1000&apiKey=" + API_KEY;

                while (url != null) {

                    System.out.println("Downloading: " + url);

                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(url))
                            .GET()
                            .build();

                    HttpResponse<String> response =
                            client.send(request, HttpResponse.BodyHandlers.ofString());

                    JSONObject root = JSON.parseObject(response.body());

                    JSONArray results = root.getJSONArray("results");
                    if (results != null && !results.isEmpty()) {
                        System.out.println(d+" total:"+results.size());
                        for (int i = 0; i < results.size(); i++) {

                            JSONObject obj = results.getJSONObject(i);

                            String ticker = obj.getString("ticker");
                            String dateStr = obj.getString("date");
                            String timeStr = obj.getString("time");

                            double actualEPS = obj.getDoubleValue("actual_eps");
                            double estimatedEPS = obj.getDoubleValue("estimated_eps");
                            double previousEPS = obj.getDoubleValue("previous_eps");

                            int importance = obj.getIntValue("importance");

                            int dateInt = Integer.parseInt(dateStr.replace("-", ""));
                            int timeInt = Integer.parseInt(timeStr.replace(":", ""));

                            ps.setInt(1, dateInt);
                            ps.setInt(2, timeInt);
                            ps.setString(3, ticker);
                            ps.setDouble(4, actualEPS);
                            ps.setDouble(5, estimatedEPS);
                            ps.setDouble(6, previousEPS);
                            ps.setInt(7, importance);

                            ps.addBatch();
                        }

                        ps.executeBatch();
                        conn.commit();
                    }
                    url = root.getString("next_url");
                }

                Thread.sleep(200);
            }

            System.out.println("Download finished.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static Connection getMySqlConnection(String db) {
        try {
            // Load MySQL JDBC driver
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("MySQL JDBC Driver not found !!");
            return null;
        }

        System.out.println("MySQL JDBC Driver Registered!");

        try {
            // Database connection info
            String dburl = "192.168.1.129"; // MySQL server IP
            String dbusr = "smart";         // username
            String dbpwd = "smart";         // password
            String dbport = "3306";         // port

            String url =
                    "jdbc:mysql://" + dburl + ":" + dbport + "/" + db;

            // Connect to database
            Connection connection =
                    DriverManager.getConnection(url, dbusr, dbpwd);

            System.out.println("SQL Connection to database established!");
            return connection;

        } catch (Exception e) {
            System.out.println("Connection Failed! Check output console");
            return null;
        }
    }
}