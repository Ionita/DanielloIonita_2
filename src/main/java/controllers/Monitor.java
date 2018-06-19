package controllers;

import entities.Message;

public class Monitor {

    public Monitor(){
        KafkaConsumer kc = new KafkaConsumer();
        kc.setAttributes(this);
        kc.runConsumer("monitor");
    }

    public void printReceivedMessage(Message st) {
        System.out.println("Day: " + st.getDay() + "\n" +
                            "Week:  " + st.getWeek() + "\n" +
                            "Year: " + st.getYear() + "\n" +
                            "value: " + st.getCount() + "\n\n");
    }
}
