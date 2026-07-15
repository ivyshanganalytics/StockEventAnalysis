package com.ronnie;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class Stock {
    public ArrayList<Qtick> qtickArray=new ArrayList<>();
    String symbol;
    public Stock() {}
    public int findQtickID(int date) {
        int from=0,to=qtickArray.size()-1;
        while(true) {
            if(to<from) return -1;
            if(to-from<=1) {
                if(qtickArray.get(from).date==date) return from;
                else if(qtickArray.get(to).date==date) return to;
                else return -1;
            }
            int n=(from+to)/2;
            if(qtickArray.get(n).date==date) return n;
            else if(qtickArray.get(n).date>date) to=n;
            else if(qtickArray.get(n).date<date) from=n;
            else return -1;
        }
    }
    public Qtick get(int i) {
        return qtickArray.get(i);
    }
    public double calcHistoricalVolatility(int site,int range) {
        double avg=0;
        int n=0;
        for(int i=site;i>site-range&&i>=1;i--) {
            double r=qtickArray.get(i).close/qtickArray.get(i-1).close-1;
            avg+=r;
            n++;
        }
        avg=avg/Math.max(1,n);
        if(n==0) return 0;
        double std=0;
        n=0;
        for(int i=site;i>site-range&&i>=1;i--) {
            double r=qtickArray.get(i).close/qtickArray.get(i-1).close-1;
            std+=Math.pow(r-avg,2);
            n++;
        }
        std=Math.sqrt(std/n)*Math.sqrt(252);
        return std;
    }
    public double calcAvgPrice(int site,int range) {
        int n=0;
        double avg=0;
        for(int k=site;k>site-range;k--) {
            if(k<0) break;
            avg+=qtickArray.get(k).close;
            n++;
        }
        avg=avg/Math.max(1,n);
        return avg;
    }
    public double calculateDiv(int from,int to) {
        double div=0;
        for(int i=Math.max(0,from);i<=Math.min(to,qtickArray.size()-1);i++) {
            div+=qtickArray.get(i).div;
        }
        return div;
    }
    public void readStockDailyDB(Connection database,String tb,int fromdate,int enddate){
        //if(qtickArray.size()>0) {
        //    qtickArray=new ArrayList<>();
        //}
        //
        if(database==null||tb==null) {
            System.out.println("database or table zero");
            return;
        }
        //
        try{
            Statement stmt=database.createStatement();
            String line=String.format("select * from %s where date>=%d and date<=%d order by date",tb,fromdate,enddate);
            ResultSet rs=stmt.executeQuery(line);
            //
            while(rs.next()) {
                Qtick q=new Qtick();
                q.date=Integer.parseInt(rs.getString(1));
                q.open=Double.parseDouble(rs.getString(2));
                q.high=Double.parseDouble(rs.getString(3));
                q.low=Double.parseDouble(rs.getString(4));
                q.close=Double.parseDouble(rs.getString(5));
                q.volume=Long.parseLong(rs.getString(6));
                q.vwap=Double.parseDouble(rs.getString(7));
                qtickArray.add(q);
            }
            rs.close();
            stmt.close();
        }
        catch(Exception e){
            System.out.println(e);
            System.out.println("stock does not exist:"+tb);
        }
    }
}
