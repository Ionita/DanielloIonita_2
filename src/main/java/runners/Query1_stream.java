package runners;

import controllers.query1.stream.FlinkController;
import controllers.query1.stream.Monitor;
import dataInjection.KafkaController;

public class Query1_stream {

    public static void main(String[] args) {
        if(args.length == 1)
            query1(args[0]);

    }

    private static void query1(String arg) {
        FlinkController fc = new FlinkController();
        KafkaController kc = new KafkaController(1);


        Thread thread_kafka_injection = new Thread(() -> {
            try {
                kc.kafkaStart(arg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


        Thread thread_query1 = new Thread(() -> {
            try {
                fc.calculateAvg();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread thread_monitor_query1 = new Thread(Monitor::new);

        thread_query1.start();
        thread_monitor_query1.start();
        try {
            Thread.sleep(10000);
            System.out.println("starting sending messages on kafka topic");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        thread_kafka_injection.start();

        try {
            thread_kafka_injection.join();
            thread_query1.join();
            thread_monitor_query1.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
