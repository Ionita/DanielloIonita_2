package runners;

import controllers.query2.stream_kafka.FlinkControllerQuery2;
import controllers.query2.stream_kafka.Monitor2;
import dataInjection.KafkaController;

public class Query2_stream {

    public static void main(String[] args) {
        if(args.length == 1)
            query2(args[0]);
    }

    private static void query2(String arg) {


        KafkaController kc = new KafkaController(2);
        FlinkControllerQuery2 query2 = new FlinkControllerQuery2();

        Thread thread_kafka_injection = new Thread(() -> {
            try {
                kc.kafkaStart(arg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread thread_query2 = new Thread(() -> {
            try {
                query2.calculateQuery2();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


        Thread thread_monitor_query2 = new Thread(Monitor2::new);


        thread_query2.start();
        thread_monitor_query2.start();
        try {
            Thread.sleep(10000);
            System.out.println("starting sending messages on kafka topic");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        thread_kafka_injection.start();

        try {
            thread_kafka_injection.join();
            thread_query2.join();
            thread_monitor_query2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}