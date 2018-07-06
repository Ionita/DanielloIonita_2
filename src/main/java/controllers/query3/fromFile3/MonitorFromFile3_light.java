package controllers.query3.fromFile3;

import controllers.query3.stream3.Query3_Item;
import entities.Message;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.TimeZone;

public class MonitorFromFile3_light {

    private static MonitorFromFile3_light instance = new MonitorFromFile3_light();

    private ArrayList<Query3_Item> query3_items;
    private final static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private Long OK_PACKETS = 0L;
    private Long DISCARDED_PACKETS = 0L;
    private int emptCyclesToCloseTheApp = 1;

    private BufferedWriter br_all;
    private BufferedWriter br_daily;
    private BufferedWriter br_weekly;

    private Integer leftBoundaryHour = -1;
    private Integer leftBoundaryDay;
    private Integer leftBoundaryWeek;
    private Integer leftBoundaryYear;

    /**
     * arriva un dato.
     * se non è settato nulla dobbiamo settare l'ora iniziale della finestra.
     * controlliamo se il post è già presente nell'ArrayList
     * se non è presente lo aggiungiamo e mettiamo il valore nella posizione corretta della sliding window.
     * se il valore supera a destra la sliding window si stampano i valori della prima colonna.
     * se il valore è precedente al primo della sliding window allora si scarta il valore.
     * insieme alla sliding window scorre anche la finestra dei primi timestamp
     *
     */

    synchronized void makeCheck(Message m){
        //field filling in message
        fillFields(m);
        //check if it is the first one
        checkFirstOne(m);
        //check if it is inside boundaries
        if(checkBoundaries(m)) //TODO <- parte importante da controllare
            //adding the value
            insertPostValue(m); //TODO <- parte importante da controllare

    }

    public static MonitorFromFile3_light getInstance(){
        return instance;
    }

    private MonitorFromFile3_light(){

        try {
            br_all = new BufferedWriter(new FileWriter("results/query_3/query3.csv", true));
            br_daily = new BufferedWriter(new FileWriter("results/query_3/query3_daily.csv", true));
            br_weekly = new BufferedWriter(new FileWriter("results/query_3/query3_weekly.csv", true));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Thread thread1 = new Thread(() -> {
            long current_ok_packets = OK_PACKETS;
            int times = 0;
            while (true) {
                try {
                    Thread.sleep(10000);
                    System.out.println("OK_PACKETS: " + OK_PACKETS);
                    System.out.println("DISCARDED_PACKETS: " + DISCARDED_PACKETS);
                    if (OK_PACKETS == current_ok_packets)
                        times ++;
                    else
                        times = 0;
                    if(times == emptCyclesToCloseTheApp){
                        if(OK_PACKETS != 0)
                            break;
                    }
                    current_ok_packets = OK_PACKETS;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if(OK_PACKETS != 0) {
                System.out.println("Error of " + ((Double.valueOf(DISCARDED_PACKETS) * 100) / Double.valueOf(OK_PACKETS)) + "%");
                try {
                    br_all.close();
                    br_daily.close();
                    br_weekly.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.exit(0);
            }
        });
        thread1.start();
    }

    private void setNewBoundaries(int positions){
        //System.out.println("position = " + positions + "\n\n\n\n\n");
        leftBoundaryHour += positions;
        while(leftBoundaryHour > 23){
            leftBoundaryHour -= 24;
            leftBoundaryDay ++;
            while(leftBoundaryDay > 7){
                leftBoundaryDay -= 7;
                leftBoundaryWeek ++;
                while(leftBoundaryWeek > 52){
                    leftBoundaryWeek -= 52;
                    leftBoundaryYear ++;
                }
            }
        }
    }

    private void initialization(){
        query3_items = new ArrayList<>();
    }

    private void fillFields(Message m){
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"));
        c.setTimeInMillis(Long.parseLong(m.getTmp()));
        m.setHour(c.get(Calendar.HOUR_OF_DAY));
        m.setDay(c.get(Calendar.DAY_OF_WEEK));
        m.setWeek(c.get(Calendar.WEEK_OF_YEAR));
        m.setMonth(c.get(Calendar.MONTH));
        m.setYear(c.get(Calendar.YEAR));
    }

    private void checkFirstOne (Message m) {
        if (leftBoundaryHour == -1) {
            initialization();
            leftBoundaryHour = m.getHour();
            leftBoundaryDay = m.getDay();
            leftBoundaryWeek = m.getWeek();
            leftBoundaryYear = m.getYear();
            /*CANCELLA*/
           /* System.out.println("setto left boundary a : \n " +
                    "day : " + leftBoundaryDay + ", hour: " + leftBoundaryHour + ", week: " + leftBoundaryWeek + ", year: " + leftBoundaryYear + "\n" +
                    "tmp of the packet: " + m.getTmp() + ", id packet: " + m.getPost_commented());
*/
            Calendar c = Calendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"));
            c.set(Calendar.WEEK_OF_YEAR, leftBoundaryWeek);
            c.set(Calendar.YEAR, leftBoundaryYear);
            c.set(Calendar.DAY_OF_WEEK, leftBoundaryDay);
            c.set(Calendar.HOUR_OF_DAY, leftBoundaryHour);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            System.out.println(dateFormat.format(c.getTimeInMillis()));
            /*CANCELLA*/

        }
    }

    private boolean checkBoundaries (Message m) {
        int messageHour = m.getHour();
        int messageDay = m.getDay();
        int messageWeek = m.getWeek();
        int messageYear = m.getYear();

        Calendar left = Calendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"));
        left.set(Calendar.HOUR_OF_DAY, leftBoundaryHour);
        left.set(Calendar.DAY_OF_WEEK, leftBoundaryDay);
        left.set(Calendar.WEEK_OF_YEAR, leftBoundaryWeek);
        left.set(Calendar.YEAR, leftBoundaryYear);

        long leftMillis = left.getTimeInMillis();

        //in the bounds
        if(messageHour == leftBoundaryHour && messageDay == leftBoundaryDay && messageWeek == leftBoundaryWeek && messageYear == leftBoundaryYear){
            OK_PACKETS++;
            return true; //si aggiunge il pacchetto alla finestra senza fare nessuna operazione preliminare
        }
        //out left
        else if (Long.parseLong(m.getTmp()) < leftMillis){
            DISCARDED_PACKETS++;
            return false; // non si aggiunge il pacchetto
        }
        //out right
        else {
            //System.out.println("fuori a destra");
            OK_PACKETS++;
            int oldHour = leftBoundaryHour, oldDay = leftBoundaryDay, oldWeek = leftBoundaryWeek, oldYear = leftBoundaryYear;

            int difference;

            if(messageDay >= leftBoundaryDay) {
                difference = messageHour + (24 * (messageDay - leftBoundaryDay))-leftBoundaryHour;
                setNewBoundaries(difference);
            }
            else if(messageWeek >= leftBoundaryWeek) {
                //week change
                difference = messageHour + (24* (messageDay + 7 - leftBoundaryDay))-leftBoundaryHour;
                setNewBoundaries(difference);
            }
            else{
                //year change

                difference = messageHour + (24* (messageDay + 7 - leftBoundaryDay))-leftBoundaryHour;
                setNewBoundaries(difference);
                System.out.println("cambio anno \n\n\n\n");
            }
            slideToRight(difference, oldHour, oldDay, oldWeek, oldYear); // ruoto verso destra la finestra e
            // salvo i dati nelle prime colonne
            // guarda bene soprattutto questa funzione
            return true;
        }

    }

    private void slideToRight(int difference, int oldHour, int oldDay, int oldWeek, int oldYear){
        //salvo tante colonne quanto è lo spostamento verso destra. faccio la stessa operazione di quando
        // setto le nuove boundaries. è brutto ma non so come fare altrimenti. Per ora questa operazione
        // server solamente a stampare il timestamp di riferimento
        for(int j = 0; j < difference; j++) {

            //ordino i dati (reverse per avere ordine decrescente)
            query3_items.sort(Comparator.comparingInt(Query3_Item::getFirstWindowPosition).reversed());
            Integer[] temp = new Integer[20];
            int k = 0;
            int maxValue = 10;
            //prendo i primi dieci valori oppure tutti gli elementi dell'array se in numero inferiore
            if (query3_items.size() < 10)
                maxValue = query3_items.size();
            //salvo i dieci valori in un array temporaneo insieme al loro id
            for (int i = 0; i  < maxValue; i++){
                temp[k] = query3_items.get(i).getUser_id().intValue();
                temp[k+1] = query3_items.get(i).getFirstWindowPosition();
                k += 2;
            }
            //rimuovo la prima colonna a tutti e aggiungo uno zero per mantenere costante la grandezza dell'array
            for (Query3_Item q : query3_items) {
                q.setDailyValue(q.getDailyValue() + q.getFirstWindowPosition());
                q.setWeekValue(q.getWeekValue() + q.getFirstWindowPosition());
                q.getSlidingWindow().remove(0);
                q.getSlidingWindow().add(0);
            }
            saveColumnToFile(temp, oldHour, oldDay, oldWeek, oldYear);
            oldHour ++;
            if(oldHour == 24){
                oldHour -= 24;
                saveDailyValues(oldDay, oldWeek, oldYear);
                oldDay ++;
                if(oldDay > 7){
                    oldDay -= 7;
                    saveWeeklyValues(oldWeek, oldYear);
                    oldWeek ++;


                    Calendar cal = Calendar.getInstance();
                    cal.set(Calendar.YEAR, oldYear);
                    cal.set(Calendar.MONTH, Calendar.DECEMBER);
                    cal.set(Calendar.DAY_OF_MONTH, 31);

                    int ordinalDay = cal.get(Calendar.DAY_OF_YEAR);
                    int weekDay = cal.get(Calendar.DAY_OF_WEEK) - 1; // Sunday = 0
                    int numberOfWeeks = (ordinalDay - weekDay + 10) / 7;

                    if(oldWeek > numberOfWeeks){
                        oldWeek -= numberOfWeeks;
                        oldYear++;
                    }
                }
            }
        }
    }

    private void insertPostValue (Message m) {
        //getting post id
        Long userID = m.getUser_id1();
        int position = checkIfAlreadyIn(userID);

        //the item doesn't already exists in the ArrayList
        if (position == -1){
            Query3_Item item = new Query3_Item(1);
            item.setUser_id(userID);
            item.setTmp(m.getTmp());
            query3_items.add(item);
            int currenthour = m.getHour();
            int currentDay = m.getDay();

            // message in the same day as left boundary
            if (currentDay == leftBoundaryDay) {
                int positionInWindow = currenthour - leftBoundaryHour;
                if (positionInWindow >= 0) {
                    int currentValue = item.getSlidingWindow().get(positionInWindow);
                    item.getSlidingWindow().set(positionInWindow, m.getCount().intValue() + currentValue);
                }
            }
            else if (currentDay >= leftBoundaryDay + 1) {
                int positionInWindow = currenthour + 24 - leftBoundaryHour;
                if (positionInWindow >= 0) {
                    int currentValue = item.getSlidingWindow().get(positionInWindow);
                    item.getSlidingWindow().set(positionInWindow, m.getCount().intValue() + currentValue);
                }
                else
                    System.out.println("position in window: " + positionInWindow);
            }
        }

        //the item is in the array
        else {
            int currenthour = m.getHour();
            int currentDay = m.getDay();
            // message in the same day as left boundary
            if (currentDay == leftBoundaryDay){
                int positionInWindow = currenthour - leftBoundaryHour;
                if (positionInWindow >= 0) {
                    int currentValue = query3_items.get(position).getSlidingWindow().get(positionInWindow);
                    query3_items.get(position).getSlidingWindow().set(positionInWindow, m.getCount().intValue() + currentValue);
                }
            }
            else if (currentDay >= leftBoundaryDay + 1){
                int positionInWindow = currenthour  + 24 - leftBoundaryHour;
                if (positionInWindow >= 0) {
                    int currentValue = query3_items.get(position).getSlidingWindow().get(positionInWindow);
                    query3_items.get(position).getSlidingWindow().set(positionInWindow, m.getCount().intValue() + currentValue);
                }
                else
                    System.out.println("position in window: " + positionInWindow);
            }
        }
    }

    private int checkIfAlreadyIn(Long post_id){
        //return the position
        int i = 0;
        for(Query3_Item q : query3_items){
            if (q.getUser_id().equals(post_id))
                return i;
            i++;
        }
        return -1;
    }

    private void saveColumnToFile(Integer[] temp, int oldHour, int oldDay, int oldWeek, int oldYear) {
        if(temp[1] != 0) {
            try {

                Calendar c = Calendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"));
                c.set(Calendar.HOUR_OF_DAY, oldHour);
                c.set(Calendar.DAY_OF_WEEK, oldDay);
                c.set(Calendar.WEEK_OF_YEAR, oldWeek);
                c.set(Calendar.YEAR, oldYear);
                c.set(Calendar.MINUTE, 0);
                c.set(Calendar.SECOND, 0);

                //BufferedWriter br = new BufferedWriter(new FileWriter("results/query_3/query3.csv", true));
                StringBuilder sb = new StringBuilder();
                sb.append(dateFormat.format(c.getTimeInMillis()));
                for (Integer i : temp) {
                    sb.append(", ");
                    sb.append(i);
                }
                sb.append(System.lineSeparator());

                br_all.write(sb.toString());
                //br.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void saveDailyValues(int oldDay, int oldWeek, int oldYear){
        query3_items.sort(Comparator.comparingInt(Query3_Item::getDailyValue).reversed());
        Integer[] temp = new Integer[20];
        int k = 0;
        int maxValue = 10;
        //prendo i primi dieci valori oppure tutti gli elementi dell'array se in numero inferiore
        if (query3_items.size() < 10)
            maxValue = query3_items.size();
        //salvo i dieci valori in un array temporaneo insieme al loro id
        for (int i = 0; i  < maxValue; i++){
            temp[k] = query3_items.get(i).getUser_id().intValue();
            temp[k+1] = query3_items.get(i).getDailyValue();
            k += 2;
        }
        for (Query3_Item q : query3_items) {
            q.setDailyValue(0);
        }

        try {
            Calendar c = Calendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"));
            c.set(Calendar.DAY_OF_WEEK, oldDay);
            c.set(Calendar.WEEK_OF_YEAR, oldWeek);
            c.set(Calendar.YEAR, oldYear);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);

            //BufferedWriter br = new BufferedWriter(new FileWriter("results/query_3/query3_daily.csv", true));
            StringBuilder sb = new StringBuilder();
            sb.append(dateFormat.format(c.getTimeInMillis()));
            for(Integer i: temp){
                sb.append(", ");
                sb.append(i);
            }
            sb.append(System.lineSeparator());

            br_daily.write(sb.toString());
            //br.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void saveWeeklyValues(int oldWeek, int oldYear){
        query3_items.sort(Comparator.comparingInt(Query3_Item::getWeekValue).reversed());
        Integer[] temp = new Integer[20];
        int k = 0;
        int maxValue = 10;
        //prendo i primi dieci valori oppure tutti gli elementi dell'array se in numero inferiore
        if (query3_items.size() < 10)
            maxValue = query3_items.size();
        //salvo i dieci valori in un array temporaneo insieme al loro id
        for (int i = 0; i  < maxValue; i++){
            temp[k] = query3_items.get(i).getUser_id().intValue();
            temp[k+1] = query3_items.get(i).getWeekValue();
            k += 2;
        }
        for (Query3_Item q : query3_items) {
            q.setWeekValue(0);
        }

        try {
            Calendar c = Calendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"));
            c.set(Calendar.WEEK_OF_YEAR, oldWeek);
            c.set(Calendar.YEAR, oldYear);
            c.set(Calendar.DAY_OF_WEEK, 1);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);

            //BufferedWriter br = new BufferedWriter(new FileWriter("results/query_3/query3_weekly.csv", true));
            StringBuilder sb = new StringBuilder();
            sb.append(dateFormat.format(c.getTimeInMillis()));
            for(Integer i: temp){
                sb.append(", ");
                sb.append(i);
            }
            sb.append(System.lineSeparator());

            br_weekly.write(sb.toString());
            //br.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
