package controllers;

import entities.Message;
import sun.plugin.javascript.navig.Array;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class Monitor {

    private ArrayList<Integer[]> list = new ArrayList<>();

    private Integer numberOfYears = 0;
    private Integer startingYear;

    public Monitor(){

        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(40000);
                int i = 0;
                for (Integer[] z : list) {
                    saveToCsv(z, i);
                    i++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        thread.start();

        KafkaConsumer kc = new KafkaConsumer();
        kc.setAttributes(this);
        kc.runConsumer("monitor");
    }

    private void saveElement(Message st) {

        if (numberOfYears == 0)
            startingYear = st.getYear();

        if (st.getYear() > startingYear + numberOfYears - 1) {
            numberOfYears++;
            list.add(new Integer[365]);
            Arrays.fill(list.get(st.getYear() - startingYear), 0);
        }

        list.get(st.getYear() - startingYear)[st.getDay() - 1] += st.getCount().intValue();
    }



    public void printReceivedMessage(Message st) {
        saveElement(st);
        /*for (Integer[] i: list) {
            for (Integer j: i)
                System.out.println("posizione: "+ i + ",valore: " + j + "\n");
        }*/


    /*System.out.println("Day: " + st.getDay() + "\n" +
                            "Week:  " + st.getWeek() + "\n" +
                            "Year: " + st.getYear() + "\n" +
                            "value: " + st.getCount() + "\n\n");
    }*/

    }

    public void saveToCsv (Integer[] deNitto, Integer position) {
        try {
            BufferedWriter br = new BufferedWriter(new FileWriter("denitto" + "_" + position + ".csv"));
            StringBuilder sb = new StringBuilder();
            int i = 0;
            for (Integer element: deNitto) {
                sb.append(i+1);
                sb.append(",");
                sb.append(element);
                sb.append(System.lineSeparator());
                i++;
            }
            br.write(sb.toString());
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
