package controllers;

import entities.Message;
import sun.util.resources.CalendarData;

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

    private Integer hour = -1;
    private Integer day = 0;
    private Integer week = 0;
    private Integer lifetime = 0;

    private Integer[] dayHours = new Integer[24];

    private Long right = 0L;
    private Long left = 0L;

    private int chour = -1;
    private int cday = -1 ;
    private int cweek = -1;
    private int cyear = -1;

    private Date firstTmpOfTheWeek = null;


    public Monitor(){
        Arrays.fill(dayHours, 0);
        KafkaConsumer kc = new KafkaConsumer();
        kc.setAttributes(this);
        kc.runConsumer("monitor2");
    }


    void makeCheck(Message m){

        //adding absent fields from timestamp
        fillFields(m);

        //initialization
        if (firstTmpOfTheWeek == null) {
            firstTmpOfTheWeek = new Date(Long.parseLong(m.getTmp()));
            //System.out.println(firstTmpOfTheWeek.toString());
        }

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
            query1Results(dateFormat.format(firstTmpOfTheWeek), dayHours);
            for(int i = 0; i<24; i++)
                dayHours[i] = 0;

            firstTmpOfTheWeek = null;
            day = 0;
            cday = m.getDay();
        }
        if (type > 2) {
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
            BufferedWriter br = new BufferedWriter(new FileWriter("query1.csv", true));
            StringBuilder sb = new StringBuilder();
            sb.append(ts);
            sb.append(", ");
            for (Integer element: value) {
                sb.append(element);
                sb.append(", ");
            }

            sb.append(System.lineSeparator());

            br.write(sb.toString());
            br.flush();
            br.close();
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