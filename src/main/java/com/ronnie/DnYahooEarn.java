package com.ronnie;

// Selenium imports: used to open a browser and interact with web pages automatically

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.List;

/**
 * This program automatically downloads US earnings calendar data
 * from Yahoo Finance and saves it into a MySQL database.
 *
 * 1) Open Yahoo Finance using a hidden browser
 * 2) Read the earnings table day by day
 * 3) Save each stock ticker and earning information into a database
 */
public class DnYahooEarn {

    public static void main(String[] args) {

        // Name of the MySQL database
        String profileDB = "stock_endofday_profile";

        // Name of the table that stores US earnings data
        String usEarnDB = "US_earning";

        // Connect to MySQL database
        Connection connection = getMySqlConnection(profileDB);

        // Path to Firefox WebDriver (this lets Java control Firefox)
        String geckodriver = "C:/smart/bin/geckodriver.exe";
        System.setProperty("webdriver.gecko.driver", geckodriver);

        // Firefox browser settings
        FirefoxOptions options = new FirefoxOptions();

        // Location of Firefox browser
        options.setBinary(String.valueOf(
                new File("C:\\Program Files\\Mozilla Firefox\\firefox.exe")));

        // Run Firefox in headless mode (no window pops up)
        options.addArguments("-headless");

        // Start the browser
        WebDriver driver = new FirefoxDriver(options);

        // Loop from 20200102 to today
        LocalDate now = LocalDate.now();
        for (int i = 0; i <= 3000; i++) {

            // get today's date
            LocalDate today= LocalDate.of(2020, 1, 2);
            // Move forward i days if i > 0
            if (i > 0) {
                today = today.plusDays(i);
            }
            if(today.isAfter(now)) break;
            // Convert date into integer format: YYYYMMDD
            int date = today.getYear() * 10000
                    + today.getMonthValue() * 100
                    + today.getDayOfMonth();

            // Date format required by Yahoo Finance URL
            DateTimeFormatter formatter =
                    DateTimeFormatter.ofPattern("yyyy-MM-dd");

            int tot = 0;   // Total records for this day
            int last = 0;  // Used to detect last page

            /**
             * Yahoo Finance paginates earnings results.
             * Each page has 100 rows, so we increase offset by 100.
             */
            for (int d = 0; d < 2000; d += 100) {

                // Build Yahoo Finance earnings calendar URL
                String link =
                        "https://finance.yahoo.com/calendar/earnings?"
                                + "day=" + today.format(formatter)
                                + "&offset=" + d
                                + "&size=100";

                System.out.println(link);

                try {
                    // Open the web page
                    driver.get(link);

                    /**
                     * Find the earnings table container using XPath.
                     * XPath is like giving very exact directions inside the webpage.
                     */
                    WebElement finCalTableDiv =
                            driver.findElement(By.xpath(
                                    "/html/body/div[2]/div[3]/main/section/section/section/section/section/section[1]/div[2]"
                            ));

                    // Find the <tbody> inside the table
                    WebElement tbodyElement =
                            finCalTableDiv.findElement(By.tagName("tbody"));

                    // Each <tr> is one company earnings row
                    List<WebElement> rows =
                            tbodyElement.findElements(By.tagName("tr"));

                    int n = 0; // Number of rows on this page

                    // Go through every row (company)
                    for (WebElement row : rows) {
                        n++;
                        tot++;

                        // Each row has multiple columns (<td>)
                        List<WebElement> columns =
                                row.findElements(By.tagName("td"));

                        // Skip if data is incomplete
                        if (columns.size() != 9) continue;
                        // Stock ticker symbol (e.g. AAPL)
                        String tick = columns.get(0).getText();
                        double estimate = parseDoubleOrZero(columns.get(4).getText());
                        double report= parseDoubleOrZero(columns.get(5).getText());

                        //
                        // Print company ticker for debugging
                        System.out.println(
                                tot + " " + date + " " + n + " "
                                        + columns.get(0).getText()+" estimate:"+estimate+" report:"+report
                        );
                        //String surprise=columns.get(6).getText();
                        // Current time as HHMMSS
                        int time = getTime();

                        // Unix timestamp (seconds)
                        long ntime = getTimeInMillisSecond() / 1000;

                        try {
                            // Create SQL statement
                            Statement stmt = connection.createStatement();

                            /**
                             * Insert or replace record in database.
                             * If the ticker already exists for that date,
                             * it will be updated.
                             */
                            String line = String.format(
                                    "replace into %s value "
                                            + "(\"%s\",%d,%d,%d,%f,%f,%d)",
                                    usEarnDB,
                                    tick,
                                    date,
                                    time,
                                    1,  // placeholder fields
                                    estimate,
                                    report,
                                    ntime
                            );

                            stmt.executeUpdate(line);
                            stmt.close();

                        } catch (SQLException e) {
                            e.printStackTrace();
                            System.exit(0);
                        }
                    }

                    // Save number of rows on this page
                    last = n;

                    // If less than 50 rows, probably no more pages
                    if (n < 50) break;

                } catch (Exception e) {
                    e.printStackTrace();

                    // Stop if page fails and data looks incomplete,
                    if (last < 100) break;
                    else last = 0;
                }
                try {
                    Thread.sleep(5000); // 10 seconds
                } catch (InterruptedException e) {
                    e.printStackTrace(); // handle if sleep is interrupted
                }
            }
        }

        // Close the browser when finished
        driver.quit();
    }

    public static double parseDoubleOrZero(String s) {
        try {
            return Double.parseDouble(s);
        } catch (Exception e) {
            return 0.0;
        }
    }
    /**
     * Returns current time in HHMMSS format.
     * Example: 14:35:20 → 143520
     */
    public static int getTime() {
        Calendar calendar = Calendar.getInstance();
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);
        return hour * 10000 + minute * 100 + second;
    }

    /**
     * Returns current time in milliseconds.
     * Useful for precise timestamps.
     */
    public static long getTimeInMillisSecond() {
        Calendar calendar = Calendar.getInstance();
        return calendar.getTimeInMillis();
    }

    /**
     * Connects to a MySQL database and returns the connection.
     */
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
