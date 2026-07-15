package com.ronnie;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class PrintOut {
    public static PrintWriter writer;
    public static boolean ready;
    public static void setFile(String file) {
        setFile(file,false);
    }
    public static void setFile(String file,boolean append) {
        if(file==null) return;
        try {
            writer = new PrintWriter(new FileWriter(file,append));
            ready=true;
        }
        catch(IOException e) {
            e.printStackTrace();
            System.out.println("can not open file to write:"+file);
            System.exit(0);
        }
    }
    public static void close() {
        if(writer!=null) {
            writer.flush();
            writer.close();
        }
    }
    public static void println(String s) {
        if(writer!=null) {
            writer.println(s);
            writer.flush();
        }
        else {
            System.out.println(s);
        }
    }
}
