package controllers;

public class Monitor {

    public Monitor(){
        KafkaConsumer kc = new KafkaConsumer();
        kc.setAttributes(this);
        kc.runConsumer("monitor");
    }

    public void printReceivedMessage(String s) {
        System.out.println("Received Message: " + s + "\n\n");
    }

}
