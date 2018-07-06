package controllers.query1.stream;

import entities.Message;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

public class Monitor {

    private final static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private Integer hour = 0;
    private Integer day = 0;
    private Integer week = 0;
    private Integer lifetime = 0;

    private BufferedWriter br_all;
    private BufferedWriter br_daily;
    private BufferedWriter br_weekly;
    private BufferedWriter br_lifetime;

    private Integer[] dayHours = new Integer[24];

    private Long right = 0L;
    private Long left = 0L;

    private int chour = -1;
    private int cday = -1 ;
    private int cweek = -1;
    private int cyear = -1;

    private Date firstTmpOfTheDay = null;
    private Date firstTmpOfTheWeek = null;
    private Date firstTmpInAbsolute = null;

    private String outputDirectory;


    public Monitor(){

        try {
            br_all      = new BufferedWriter(new FileWriter("results/query_1/query1.csv", true));
            br_daily    = new BufferedWriter(new FileWriter("results/query_1/query1_daily.csv", true));
            br_weekly   = new BufferedWriter(new FileWriter("results/query_1/query1_weekly.csv", true));
            br_lifetime = new BufferedWriter(new FileWriter("results/query_1/query1_lifetime.csv", true));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Thread thread1 = new Thread(() -> {
            int currentTotalValue = lifetime;
            while (true) {
                try {
                    Thread.sleep(10000);
                    if(currentTotalValue == lifetime && lifetime != 0){
                        lifetime_query(dateFormat.format(firstTmpInAbsolute), lifetime);
                        System.exit(0);
                        try {
                            br_all.close();
                            br_daily.close();
                            br_weekly.close();
                            br_lifetime.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    currentTotalValue = lifetime;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread1.start();
        Arrays.fill(dayHours, 0);
        KafkaConsumer kc = new KafkaConsumer();
        kc.setAttributes(this);
        kc.runConsumer("monitor");
    }

    void makeCheck(Message m){

        //adding absent fields from timestamp
        fillFields(m);

        //initialization
        if (firstTmpOfTheDay == null)
            firstTmpOfTheDay = new Date(Long.parseLong(m.getTmp()));
        if (firstTmpOfTheWeek == null)
            firstTmpOfTheWeek = new Date(Long.parseLong(m.getTmp()));
        if (firstTmpInAbsolute== null)
            firstTmpInAbsolute = new Date(Long.parseLong(m.getTmp()));

        if (chour == -1){
            chour = m.getHour();
            cday = m.getDay();
            cweek = m.getWeek();
            cyear = m.getYear();
        }

        //same hour, day, week, year
        if(m.getHour().equals(chour) && m.getDay().equals(cday) && m.getWeek().equals(cweek)){
            sumAll(0, m);
        }
        // next hour, same day, week, year
        else if(m.getHour() > chour && m.getDay().equals(cday) && m.getWeek().equals(cweek)){
            sumAll(1, m);
        }
        //next day, same week, year
        else if(m.getDay() > cday && m.getWeek().equals(cweek)){
            sumAll(2, m);
        }
        // next week, same year
        else if(m.getWeek() > cweek){
            sumAll(3, m);
        }
        else if (m.getWeek() < cweek) {
            chour = m.getHour();
            cday = m.getDay();
            cweek = m.getWeek();
            cyear = m.getYear();
            sumAll(0, m);
        }
        //error
        else
            System.out.println("currentHour: " + chour + ", m.hour: " + m.getHour() + ", currentDay: " + cday + ", m.day: " + m.getDay() + ", currentWeek: " + cweek+ ", m.week: "+ m.getWeek()+ ", currentYear: " + cyear + ", m.year: " + m.getYear());

    }

    private void sumAll(int type, Message m){
        if(type > 0) {
            dayHours[chour] = hour;
            hour = 0;
            chour = m.getHour();
        }
        if (type > 1) {
            query1Results(dateFormat.format(firstTmpOfTheDay), dayHours);
            for(int i = 0; i<24; i++)
                dayHours[i] = 0;

            daily_query1results(dateFormat.format(firstTmpOfTheDay), day);
            firstTmpOfTheDay = new Date(Long.parseLong(m.getTmp()));
            day = 0;
            cday = m.getDay();
        }
        if (type > 2) {
            weekly_query1results(dateFormat.format(firstTmpOfTheWeek), week, firstTmpOfTheWeek);
            firstTmpOfTheWeek = new Date(Long.parseLong(m.getTmp()));
            week = 0;
            cweek = m.getWeek();
        }

        hour += m.getCount().intValue();
        day += m.getCount().intValue();
        week += m.getCount().intValue();
        lifetime += m.getCount().intValue();
    }

    private void query1Results (String ts, Integer[] value) {

        try {
            //BufferedWriter br = new BufferedWriter(new FileWriter("results/query_1/query1.csv", true));
            StringBuilder sb = new StringBuilder();
            sb.append(ts);
            for (Integer element: value) {
                sb.append(", ");
                sb.append(element);
            }

            sb.append(System.lineSeparator());

            br_all.write(sb.toString());
            br_all.flush();
            //br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void daily_query1results(String ts, Integer day){
        try {
            //BufferedWriter br = new BufferedWriter(new FileWriter("results/query_1/query1_daily.csv", true));
            String sb = ts +
                    ", " +
                    day +
                    System.lineSeparator();
            br_daily.write(sb);
            br_daily.flush();
            //br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void weekly_query1results(String ts, Integer week, Date firstTmpOfTheWeek){
        try {
            //BufferedWriter br = new BufferedWriter(new FileWriter("results/query_1/query1_weekly.csv", true));
            String sb = ts +
                    ", " +
                    week +
                    System.lineSeparator();
            br_weekly.write(sb);
            br_weekly.flush();
            //br.close();

            Calendar c = Calendar.getInstance();
            c.setTime(firstTmpOfTheWeek);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void lifetime_query(String ts, Integer lifetime){
        try {
            //BufferedWriter br = new BufferedWriter(new FileWriter("results/query_1/query1_lifetime.csv", true));
            String sb = ts +
                    ", " +
                    lifetime +
                    System.lineSeparator();
            br_lifetime.write(sb);
            br_lifetime.flush();
            //br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void fillFields(Message m){
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(Long.parseLong(m.getTmp()));
        m.setHour(c.get(Calendar.HOUR_OF_DAY));
        m.setDay(c.get(Calendar.DAY_OF_WEEK));
        m.setWeek(c.get(Calendar.WEEK_OF_YEAR));
        m.setMonth(c.get(Calendar.MONTH));
        m.setYear(c.get(Calendar.YEAR));
    }

}