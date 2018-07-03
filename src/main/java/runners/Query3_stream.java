package runners;

import controllers.query3.stream.FlinkControllerQuery3;
import controllers.query3.stream.Monitor3;
import dataInjection.KafkaController;

public class Query3_stream {

    public static void main(String[] args) {
        query3();
    }

    private static void query3() {


        KafkaController kc = new KafkaController(3);
        FlinkControllerQuery3 query3 = new FlinkControllerQuery3();

        Thread thread_kafka_injection = new Thread(() -> {
            try {
                kc.kafkaStart();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread thread_query3 = new Thread(() -> {
            try {
                query3.calculateQuery3();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


        Thread thread_monitor_query3 = new Thread(Monitor3::new);
        thread_query3.start();
        thread_monitor_query3.start();
        try {
            Thread.sleep(10000);
            System.out.println("starting sending messages on kafka topic");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        thread_kafka_injection.start();

        try {
            thread_kafka_injection.join();
            thread_query3.join();
            thread_monitor_query3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
