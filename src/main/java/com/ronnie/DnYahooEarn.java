package com.ronnie;

//import datacenter.DataCenter;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;
//import util.MyString;
//import util.MyTime;
//import util.PushOver;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.Calendar;
import java.util.List;

public class DnYahooEarn {
    public static void main(String[] args) {
        String usEarnDB="US_earning"; //this is the table name
        String profileDB="stock_endofday_profile"; //this is the database name

        Connection connection=getMySqlConnection(profileDB);
        String geckodriver="C:/smart/bin/geckodriver.exe";
        System.setProperty("webdriver.gecko.driver",geckodriver);
        //System.setProperty("webdriver.gecko.driver","C:\\Users\\xiangz\\Documents\\jdks\\chrome\\geckodriver.exe" );
        FirefoxOptions options = new FirefoxOptions();
        options.setBinary(String.valueOf(new File("C:\\Program Files\\Mozilla Firefox\\firefox.exe"))); // Adjust the path as needed
        //
        options.addArguments("-headless");
        //
        WebDriver driver = new FirefoxDriver(options);
        //int date = MyTime.getDate();
        int ntot=0,ntot0=0;
        for(int i=0;i<=30;i++) {
            LocalDate today = LocalDate.now();
            if (i>0) {
                today = today.plusDays(i);
            }
            int date=today.getYear()*10000+today.getMonthValue()*100+today.getDayOfMonth();
            // Find the Sunday of the current week
            LocalDate sunday = today.with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY));
            // Find the Saturday of the current week
            LocalDate saturday = today.with(TemporalAdjusters.nextOrSame(DayOfWeek.SATURDAY));
            // Define the date format
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            int tot=0;
            int last=0;
            for(int d=0;d<2000;d+=100) {
                //String link = "https://finance.yahoo.com/calendar/earnings?from=" + sunday.format(formatter) + "&to=" + saturday.format(formatter) + "&day=" + today.format(formatter) + "&offset=" +d+ "&size=100";
                String link = "https://finance.yahoo.com/calendar/earnings?"+"day=" + today.format(formatter) + "&offset=" +d+ "&size=100";
                //
                System.out.println(link);
                try {
                    driver.get(link);
                    ///html/body/div[2]/main/section/section/section/article/main/main/div[2]
                    ///html/body/div[2]/main/section/section/section/article/main/main/div[2]
                    //WebElement finCalTableDiv = driver.findElement(By.cssSelector("table.yf-2twxe2"));
                    ///html/body/div[2]/main/section/section/section/article/section/section[1]/div[2]
                    //WebElement finCalTableDiv =driver.findElement(By.xpath("/html/body/div[2]/main/section/section/section/article/main/main/div[2]"));
                    WebElement finCalTableDiv =driver.findElement(By.xpath("/html/body/div[2]/div[3]/main/section/section/section/section/section/section[1]/div[2]"));
                    //System.out.println(finCalTableDiv.getText());
                    //WebElement divElement = finCalTableDiv.findElement(By.cssSelector("div.Ovx\\(a\\).Ovx\\(h\\)--print.Ovy\\(h\\).W\\(100\\%\\)"));
                    //System.out.println(divElement.getText());
                    ///html/body/div[2]/main/section/section/section/section/section/section[1]/div[2]
                    WebElement tbodyElement = finCalTableDiv.findElement(By.tagName("tbody"));
                    //System.out.println(tbodyElement.getText());
                    //
                    List<WebElement> rows = tbodyElement.findElements(By.tagName("tr"));
                    // Iterate through the rows and print their text
                    int n = 0;
                    for (WebElement row : rows) {
                        n++;
                        tot++;
                        ntot++;
                        if(i==0) ntot0++;
                        List<WebElement> columns = row.findElements(By.tagName("td"));
                        if (columns.size() != 9) continue;
                        System.out.println(tot + " "+date+" "+ n + " " + columns.get(0).getText());
                        //
                        String tick = columns.get(0).getText();
                        int time = getTime();
                        long ntime = getTimeInMillisSecond() / 1000;
                        try {
                            Statement stmt = connection.createStatement();
                            String line = String.format("replace into %s value (\"%s\",%d,%d,%d,%d,%d,%d)", usEarnDB, tick, date, time, 1, 0, 0, ntime);
                            stmt.executeUpdate(line);
                            stmt.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                            System.exit(0);
                        }
                    }
                    last=n;
                    if(n<50) break;
                }
                catch(Exception e) {
                    System.out.println(e.fillInStackTrace());
                    if(last<100) break;
                    else last=0;
                }
            }
            int x=1;
        }
        driver.quit();
        if(ntot>0) return;
    }
    public static int getTime(){
        Calendar calendar = Calendar.getInstance();
        int hour=calendar.get(Calendar.HOUR_OF_DAY);
        int minute=calendar.get(Calendar.MINUTE);
        int second=calendar.get(Calendar.SECOND);
        int time=(hour)*10000+(minute)*100+second;
        //int dayofweek=calendar.get(Calendar.DAY_OF_WEEK);
        return time;
    }
    public static long getTimeInMillisSecond(){
        Calendar calendar = Calendar.getInstance();
        long tt=calendar.getTimeInMillis();
        return tt;
    }
    public static Connection getMySqlConnection(String db) {
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
            String dburl="192.168.1.129";
            String dbusr="smart";
            String dbpwd="smart";
            String dbport="3306";
            String url="jdbc:mysql://"+dburl+":"+dbport+"/"+db;
            connection = DriverManager.getConnection(url, dbusr, dbpwd);
            System.out.println("SQL Connection to database established!");
        } catch (Exception e) {
            System.out.println("Connection Failed! Check output console");
            return null;
        }
        return connection;
    }
}
