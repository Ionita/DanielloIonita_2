package controllers.query2;

import controllers.query1.KafkaConsumer;
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
        fillFields(m);      //v
        //check if it is the first one
        checkFirstOne(m);   //v
        //check if it is inside boundaries
        checkBoundaries(m);
        //adding the value
        insertPostValue(m);

    }

    private void setNewBoundaries(int positions){
        leftBoundaryHour += positions;
        if(leftBoundaryHour > 23){
            leftBoundaryHour -= 24;
            leftBoundaryDay ++;
            if(leftBoundaryDay > 7){
                leftBoundaryDay -= 7;
                leftBoundaryWeek ++;
                if(leftBoundaryWeek > 52){
                    leftBoundaryWeek -= 52;
                    leftBoundaryYear ++;
                }
            }
        }

        rightBoundaryHour = leftBoundaryHour + slidingWindowSize;
        if(rightBoundaryHour > 23){
            rightBoundaryHour -= 24;
            rightBoundaryDay ++;
            if(rightBoundaryDay > 7){
                rightBoundaryDay -= 7;
                rightBoundaryWeek ++;
                if(rightBoundaryWeek > 52){
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
            rightBoundaryHour = leftBoundaryHour + slidingWindowSize;
            if (rightBoundaryHour >= 24){
                rightBoundaryHour -= 24;
                rightBoundaryDay = leftBoundaryDay + 1;
            }
            else
                rightBoundaryDay = leftBoundaryDay;
        }
    }

    private void checkBoundaries (Message m) {
        if (m.getHour() < hour && m.getDay() <= leftBoundaryDay) {
            System.out.println("Tmp minore dell'attuale, pacchetto scartavetrato");
        }
        else if (m.getHour() > (rightBoundaryHour) && m.getDay().equals(rightBoundaryDay)){
            System.out.println("chiamiamo slideToRight");
            //move the left boundary
            leftBoundaryHour += m.getHour() - rightBoundaryHour;
            for(int i = 0; i < m.getHour() - rightBoundaryHour; i++)
                //sliding the window
                slideToRight();
            //move the right boundary
            rightBoundaryDay = leftBoundaryHour + slidingWindowSize;
            if (rightBoundaryHour >= 24){
                rightBoundaryHour -= 24;
                rightBoundaryDay = leftBoundaryDay + 1;
            }
        }
    }

    private void slideToRight(){
        Integer[] temp = new Integer[slidingWindowSize];
        int i = 0;
        for(Query2_Item q : query2_items){
            temp[i] = q.getSlidingWindow().get(0);
            q.getSlidingWindow().remove(0);
            i++;
        }
        saveColumnToFile(tmpSlidingWindows.get(0), temp);
        tmpSlidingWindows.remove(0);
    }

    private void saveColumnToFile(Integer integer, Integer[] temp) {

    }

    private void insertPostValue (Message m) {
        Long postID = m.getPost_commented();
        int position = checkIfAlreadyIn(postID);
        if (position == -1)
            System.out.println("aggiungi");
        else
            System.out.println("metti nella posizione corretta");
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