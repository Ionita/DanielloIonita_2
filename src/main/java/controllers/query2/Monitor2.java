package controllers.query2;

import entities.Message;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Monitor2 {


    private ArrayList<Query2_Item> query2_items;
    private final static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private Long OK_PACKETS = 0L;
    private Long DISCARDED_PACKETS = 0L;

    private Integer leftBoundaryHour = -1;
    private Integer leftBoundaryDay;
    private Integer leftBoundaryWeek;
    private Integer leftBoundaryYear;


    private Integer rightBoundaryHour;
    private Integer rightBoundaryDay;
    private Integer rightBoundaryWeek;
    private Integer rightBoundaryYear;

    private final Integer slidingWindowSize = 24;


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

    void makeCheck(Message m){
        //field filling in message
        fillFields(m);
        //check if it is the first one
        checkFirstOne(m);
        //check if it is inside boundaries
        if(checkBoundaries(m)) //TODO <- parte importante da controllare
            //adding the value
            insertPostValue(m); //TODO <- parte importante da controllare

    }

    public Monitor2(){
        Thread thread1 = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10000);
                    System.out.println("OK_PACKETS: " + OK_PACKETS);
                    System.out.println("DISCARDED_PACKETS: " + DISCARDED_PACKETS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread1.start();
        KafkaConsumer kc = new KafkaConsumer();
        kc.setAttributes(this);
        kc.runConsumer("monitor_query2");
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

        rightBoundaryHour += positions;
        while(rightBoundaryHour > 23){
            rightBoundaryHour -= 24;
            rightBoundaryDay = rightBoundaryDay + 1;
            while(rightBoundaryDay > 7){
                rightBoundaryDay -= 7;
                rightBoundaryWeek ++;
                while(rightBoundaryWeek > 52){
                    rightBoundaryWeek -= 52;
                    rightBoundaryYear ++;
                }
            }
        }
    }

    private void initialization(){
        query2_items = new ArrayList<>();
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

    private void checkFirstOne (Message m) {
        if (leftBoundaryHour == -1) {
            initialization();
            if (slidingWindowSize > 24){
                System.out.println("errore > 24");
                return;
            }
            leftBoundaryHour = m.getHour();
            leftBoundaryDay = m.getDay();
            leftBoundaryWeek = m.getWeek();
            leftBoundaryYear = m.getYear();

            rightBoundaryHour = leftBoundaryHour + slidingWindowSize;
            rightBoundaryDay = leftBoundaryDay;
            rightBoundaryWeek = leftBoundaryWeek;
            rightBoundaryYear = leftBoundaryYear;
            if(rightBoundaryHour > 23){
                rightBoundaryHour -= 24;
                rightBoundaryDay = leftBoundaryDay + 1;
                if(rightBoundaryDay > 7){
                    rightBoundaryDay -= 7;
                    rightBoundaryWeek = leftBoundaryWeek + 1;
                    if(rightBoundaryWeek > 52){
                        rightBoundaryWeek -= 52;
                        rightBoundaryYear = leftBoundaryYear + 1;
                    }
                }
            }
        }
    }

    private boolean checkBoundaries (Message m) {
        int messageHour = m.getHour();
        int messageDay = m.getDay();
        int messageWeek = m.getWeek();
        int messageYear = m.getYear();

        Calendar left = Calendar.getInstance();
        left.set(Calendar.HOUR_OF_DAY, leftBoundaryHour);
        left.set(Calendar.DAY_OF_WEEK, leftBoundaryDay);
        left.set(Calendar.WEEK_OF_YEAR, leftBoundaryWeek);
        left.set(Calendar.YEAR, leftBoundaryYear);

        long leftMillis = left.getTimeInMillis();

        Calendar right = Calendar.getInstance();
        right.set(Calendar.HOUR_OF_DAY, rightBoundaryHour);
        right.set(Calendar.DAY_OF_WEEK, rightBoundaryDay);
        right.set(Calendar.WEEK_OF_YEAR, rightBoundaryWeek);
        right.set(Calendar.YEAR, rightBoundaryYear);

        long rightMillis = right.getTimeInMillis();

        //in the bounds
        if(messageHour == leftBoundaryHour && messageDay == leftBoundaryDay && messageWeek == leftBoundaryWeek && messageYear == leftBoundaryYear){
            OK_PACKETS++;
            return true; //si aggiunge il pacchetto alla finestra senza fare nessuna operazione preliminare
        }
        //in the bounds
        else if(Long.parseLong(m.getTmp()) <= rightMillis && Long.parseLong(m.getTmp()) >= leftMillis){
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

            if(messageDay >= rightBoundaryDay) {
                difference = messageHour + (24 * (messageDay - rightBoundaryDay))-rightBoundaryHour;
                setNewBoundaries(difference);
            }
            else if(messageWeek >= rightBoundaryWeek) {
                //week change
                difference = messageHour + (24* (messageDay + 7 - rightBoundaryDay))-rightBoundaryHour;
                setNewBoundaries(difference);
            }
            else{
                //year change

                difference = messageHour + (24* (messageDay + 7 - rightBoundaryDay))-rightBoundaryHour;
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

            oldHour ++;
            if(oldHour == 24){
                oldHour -= 24;
                oldDay ++;
                if(oldDay > 7){
                    oldDay -= 7;
                    oldWeek ++;
                    if(oldWeek > 52){
                        oldWeek -= 52;
                        oldYear++;
                    }
                }
            }
            //ordino i dati (reverse per avere ordine decrescente)
            query2_items.sort(Comparator.comparingInt(Query2_Item::getFirstWindowPosition).reversed());
            Integer[] temp = new Integer[20];
            int k = 0;
            int maxValue = 10;
            //prendo i primi dieci valori oppure tutti gli elementi dell'array se in numero inferiore
            if (query2_items.size() < 10)
                maxValue = query2_items.size();
            //salvo i dieci valori in un array temporaneo insieme al loro id
            for (int i = 0; i  < maxValue; i++){
                temp[k] = query2_items.get(i).getPost_id().intValue();
                temp[k+1] = query2_items.get(i).getFirstWindowPosition();
                k += 2;
            }
            //rimuovo la prima colonna a tutti e aggiungo uno zero per mantenere costante la grandezza dell'array
            for (Query2_Item q : query2_items) {
                q.getSlidingWindow().remove(0);
                q.getSlidingWindow().add(0);
            }
            //per ora stampo su schermo e basta
            saveColumnToFile(temp, oldHour, oldDay, oldWeek, oldYear);
            //tmpSlidingWindows.remove(0);
        }
    }

    private void saveColumnToFile(Integer[] temp, int oldHour, int oldDay, int oldWeek, int oldYear) {
        try {
            Calendar c = Calendar.getInstance();
            c.set(Calendar.HOUR, oldHour);
            c.set(Calendar.DAY_OF_WEEK, oldDay);
            c.set(Calendar.WEEK_OF_YEAR, oldWeek);
            c.set(Calendar.YEAR, oldYear);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);

            BufferedWriter br = new BufferedWriter(new FileWriter("query2.csv", true));
            StringBuilder sb = new StringBuilder();
            sb.append(dateFormat.format(c.getTimeInMillis()));
            for(Integer i: temp){
                sb.append(", ");
                sb.append(i);
            }
            sb.append(System.lineSeparator());

            br.write(sb.toString());
            br.flush();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void insertPostValue (Message m) {
        //getting post id
        Long postID = m.getPost_commented();
        int position = checkIfAlreadyIn(postID);

        //the item doesn't already exists in the ArrayList
        if (position == -1){
            Query2_Item item = new Query2_Item(slidingWindowSize);
            item.setPost_id(postID);
            item.setTmp(m.getTmp());
            query2_items.add(item);
            int currenthour = m.getHour();
            int currentDay = m.getDay();

            // message in the same day as left boundary
            if (currentDay == leftBoundaryDay){
                int positionInWindow = currenthour - leftBoundaryHour;
                int currentValue = item.getSlidingWindow().get(positionInWindow);
                item.getSlidingWindow().set(positionInWindow, m.getCount().intValue() + currentValue);
            }
            else if (currentDay >= leftBoundaryDay + 1){
                int positionInWindow = currenthour  + 23 - leftBoundaryHour; //TODO-------------rivedi bene se è 23 o 24
                int currentValue = item.getSlidingWindow().get(positionInWindow);
                item.getSlidingWindow().set(positionInWindow, m.getCount().intValue() + currentValue);
            }
        }

        //the item is in the array
        else {
            int currenthour = m.getHour();
            int currentDay = m.getDay();
            // message in the same day as left boundary
            if (currentDay == leftBoundaryDay){
                int positionInWindow = currenthour - leftBoundaryHour;
                int currentValue = query2_items.get(position).getSlidingWindow().get(positionInWindow);
                query2_items.get(position).getSlidingWindow().set(positionInWindow, m.getCount().intValue() + currentValue);
            }
            else if (currentDay >= leftBoundaryDay + 1){
                int positionInWindow = currenthour  + 23 - leftBoundaryHour; //TODO-------------rivedi bene se è 23 o 24
                int currentValue = query2_items.get(position).getSlidingWindow().get(positionInWindow);
                query2_items.get(position).getSlidingWindow().set(positionInWindow, m.getCount().intValue() + currentValue);
            }
        }
    }

    private int checkIfAlreadyIn(Long post_id){
        //return the position
        int i = 0;
        for(Query2_Item q : query2_items){
            if (q.getPost_id().equals(post_id))
                return i;
            i++;
        }
        return -1;
    }





    private void printBoundaries(){

        System.out.println("left boundaries: ");
        System.out.println("hour: " + leftBoundaryHour
                + ", day: " + leftBoundaryDay
                + ", week: " + leftBoundaryWeek
                + ", year: " + leftBoundaryYear);
        System.out.println("right boundaries: ");
        System.out.println("hour: " + rightBoundaryHour
                + ", day: " + rightBoundaryDay
                + ", week: " + rightBoundaryWeek
                + ", year: " + rightBoundaryYear);
        System.out.println("OK_PACKETS: " + OK_PACKETS);
        System.out.println("DISCARDED_PACKETS: " + DISCARDED_PACKETS);
    }

    private void printMessage(Message m){
        System.out.println("message: " + m.getPost_commented());
        System.out.println("hour: " + m.getHour()
                + ", day: " + m.getDay()
                + ", week: " + m.getWeek()
                + ", year: " + m.getYear());
        System.out.println("\n\n\n");
    }

    private void printWindow(String id, String window){
        String reference = "644564";
        if(id.equals(reference))
            System.out.println("id: " + id + ", " + window);
    }

}