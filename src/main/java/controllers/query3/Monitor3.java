package controllers.query3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Monitor3 {

    private final static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private ArrayList<Item> items = new ArrayList<>();
    private final String filename = "query3.csv";

    public Monitor3(){
        createFile(filename);

    }


    private void createFile (String filename) {

        File f = new File(filename);
        try {
            f.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private void saveTopColumn () throws IOException {


        if (items.size() == 0)
            return;

        BufferedWriter br = new BufferedWriter(new FileWriter("query3.csv", true));
        StringBuilder sb = new StringBuilder();

        items.sort(Comparator.comparingInt(Item::returnFirstItem));
        addTmpToFile(sb, "todo");

        if (items.size() < 10) {
            //save element


            for (Item i: items) {
                Integer x = i.slidingWindow.get(0);
                i.slidingWindow.remove(0);
                saveIntegerToFile(sb, x, i.getId());
            }

        }
        else {
            for (int i = 0; i < 10; i++) {
                Integer x = items.get(i).returnFirstItem();
                items.get(i).slidingWindow.remove(0);
                saveIntegerToFile(sb, x, items.get(i).getId());
            }
        }
        closeFile(sb, br);
    }

    private void addTmpToFile(StringBuilder sb, String tmp) {
        System.out.println("to Implement add tmp file\n");

        sb.append(tmp);
        sb.append(", ");

    }

    private void closeFile(StringBuilder sb, BufferedWriter br) throws IOException {
        sb.append(System.lineSeparator());
        br.write(sb.toString());
        br.flush();
        br.close();
    }

    private void saveIntegerToFile(StringBuilder sb, Integer x, Long id) {
        System.out.println("to Implement save integer to file\n");

        sb.append(id);
        sb.append(", ");
        sb.append(x);
        sb.append(", ");
    }

    /*private final static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");





    private Date firstTmpOfTheWeek = null;


    public Monitor3(){
        Arrays.fill(dayHours, 0);
        KafkaConsumer kc = new KafkaConsumer();
        kc.setAttributes(this);
        kc.runConsumer("monitor3");
    }


    void makeCheck(Message m){

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
    }*/

}