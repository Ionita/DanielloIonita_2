package controllers.query2;

import entities.Message;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

public class Monitor2 {


    ArrayList<Query2_Item> query2_items;
    private final static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private Long firstTmp;
    private ArrayList<Integer> tmpSlidingWindows;


    private Integer hour = -1;
    private Integer day = 0;
    private Integer week = 0;
    private Integer lifetime = 0;


    private Integer leftBoundaryHour;
    private Integer leftBoundaryDay;
    private Integer leftBoundaryWeek;
    private Integer leftBoundaryYear;


    private Integer rightBoundaryHour;
    private Integer rightBoundaryDay;
    private Integer rightBoundaryWeek;
    private Integer rightBoundaryYear;

    private final Integer slidingWindowSize = 24;


    public Monitor2(){
        KafkaConsumer kc = new KafkaConsumer();
        kc.setAttributes(this);
        kc.runConsumer("monitor_query2");
    }

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

    public void makeCheck(Message m){
        //field filling in message
        fillFields(m);
        //check if it is the first one
        checkFirstOne(m);
        //check if it is inside boundaries
        checkBoundaries(m);
        //adding the value
        //insertPostValue(m);

    }

    private void setNewBoundaries(int positions){
        System.out.println("position = " + positions + "\n\n\n\n\n");
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
            System.out.println("sommo un giorno ***************************************************");
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
        tmpSlidingWindows = new ArrayList<>();
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
        if (hour == -1) {
            initialization();
            if (slidingWindowSize > 24){
                System.out.println("errore > 24");
                return;
            }
            hour = m.getHour();
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

    private void checkBoundaries (Message m) {
        int messageHour = m.getHour();
        int messageDay = m.getDay();
        int messageWeek = m.getWeek();
        int messageYear = m.getYear();

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


        if(Long.parseLong(m.getTmp()) <= rightMillis && Long.parseLong(m.getTmp()) >= leftMillis){
            System.out.println("it is in bound");
        }
        else if (Long.parseLong(m.getTmp()) < leftMillis){
            System.out.println("fuori a sinistra");

            System.out.println("hour: " + m.getHour()
                    + ", day: " + m.getDay()
                    + ", week: " + m.getWeek()
                    + ", year: " + m.getYear()
                    + ", left millis: " + leftMillis
                    + ", right millis: " + rightMillis
                    + ", current millis: " + m.getTmp());
            System.out.println("\n\n\n");

        }
        else {
            System.out.println("fuori a destra");
            System.out.println("hour: " + m.getHour()
                    + ", day: " + m.getDay()
                    + ", week: " + m.getWeek()
                    + ", year: " + m.getYear()
                    + ", left millis: " + leftMillis
                    + ", right millis: " + rightMillis
                    + ", current millis: " + m.getTmp());
            System.out.println("\n\n\n");

            if(messageDay >= rightBoundaryDay)
                setNewBoundaries(messageHour + (24* (messageDay - rightBoundaryDay))-rightBoundaryHour);
            else if(messageWeek >= rightBoundaryWeek)
                System.out.println("cambio settimana\n\n\n");

            /*if(messageDay == rightBoundaryDay){
                setNewBoundaries(messageHour-rightBoundaryHour);
            }
            else if(messageDay == rightBoundaryDay + 1){
                setNewBoundaries(messageHour + 24 - rightBoundaryHour);
            }
            else{
                System.out.println("bound troppo alto \n\n\n\n");
            }*/
        }
/*
        //message in the bounds
        if ((messageHour >= leftBoundaryHour && messageDay >= leftBoundaryDay && messageWeek >= leftBoundaryWeek && messageYear >= leftBoundaryYear) &&
                (messageHour <= rightBoundaryHour && messageDay <= rightBoundaryDay && messageWeek <= rightBoundaryWeek && messageYear <= rightBoundaryYear)){
            System.out.println("sono nel bound");
        }
        //message outbound left
        else if (messageHour < leftBoundaryHour && messageDay <= leftBoundaryDay && messageWeek <= leftBoundaryWeek && messageYear <= leftBoundaryYear){
            System.out.println("message out of bound left");
        }
        //message out of bound right
        else {
            System.out.println("message out of bound right, hour: " + m.getHour() + ", day: " + m.getDay());
            int difference;
            //message in the same day
            if (messageDay == rightBoundaryDay){
                difference = messageHour - rightBoundaryHour;
                for(int j = 0; j< difference; j++)
                    slideToRight();
                setNewBoundaries(difference);
            }
            //else if the day after
            else if (messageDay == rightBoundaryDay + 1){
                difference = messageHour + 24 - rightBoundaryHour;
                for(int j = 0; j< difference; j++)
                    slideToRight();
                setNewBoundaries(difference);
            }
            else
                System.out.println("scarta, troppa differenza");
        }
        */
    }

    private void slideToRight(){
        Integer[] temp = new Integer[slidingWindowSize];
        int i = 0;
        for(Query2_Item q : query2_items){
            temp[i] = q.getSlidingWindow().get(0);
            q.getSlidingWindow().remove(0);
            i++;
        }
        //saveColumnToFile(tmpSlidingWindows.get(0), temp);
        saveColumnToFile(0, temp);
        //tmpSlidingWindows.remove(0);
    }

    private void saveColumnToFile(Integer integer, Integer[] temp) {
        for(Query2_Item q : query2_items){
            System.out.println(q.getPost_id() + ": " + q.getSlidingWindow().toString());
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
            System.out.println("current hour = " + currenthour);
            int currentDay = m.getDay();
            // message in the same day as left boundary
            if (currentDay == leftBoundaryDay){
                int positionInWindow = currenthour - leftBoundaryHour;
                System.out.println("cerco la posizione: " + positionInWindow);
                System.out.println("nell'array: " + item.getSlidingWindow().toString());
                int currentValue = item.getSlidingWindow().get(positionInWindow);
                item.getSlidingWindow().set(positionInWindow, m.getCount().intValue() + currentValue);
            }
            else if (currentDay >= leftBoundaryDay + 1){
                int positionInWindow = currenthour  + 24 - leftBoundaryHour;
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
                int positionInWindow = currenthour  + 24 - leftBoundaryHour;
                int currentValue = query2_items.get(position).getSlidingWindow().get(positionInWindow);
                query2_items.get(position).getSlidingWindow().set(positionInWindow, m.getCount().intValue() + currentValue);
            }
            System.out.println("metti nella posizione corretta");
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

}