import controllers.query1.FlinkController;
import controllers.query2.FlinkControllerQuery2;
import controllers.query2.Monitor2;
import controllers.query3.FlinkControllerQuery3;
import dataInjection.KafkaController;
import controllers.query1.Monitor;


public class Runner {

    public static void main(String[] args) throws InterruptedException {
        query2();
    }

    private static void query1() throws InterruptedException {
        FlinkController fc = new FlinkController();
        KafkaController kc = new KafkaController(1);


        Thread thread_kafka_injection = new Thread(() -> {
            try {
                kc.kafkaStart();
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
        thread_kafka_injection.start();
        thread_monitor_query1.start();

        thread_kafka_injection.join();
        thread_query1.join();
        thread_monitor_query1.join();


    }

    private static void query2() throws InterruptedException {

        KafkaController kc = new KafkaController(2);
        FlinkControllerQuery2 query2 = new FlinkControllerQuery2();

        Thread thread_kafka_injection = new Thread(() -> {
            try {
                kc.kafkaStart();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread thread_query2= new Thread(() -> {
            try {
                query2.calculateQuery2();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread thread_monitor_query2 = new Thread(Monitor2::new);


        thread_monitor_query2.start();
        thread_query2.start();
        try {
            Thread.sleep(20000);
            System.out.println("starting sending messages on kafka topic");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        thread_kafka_injection.start();

        thread_kafka_injection.join();
        thread_query2.join();
        thread_monitor_query2.join();
    }

    private static void query3() throws InterruptedException {

        KafkaController kc = new KafkaController(3);
        FlinkControllerQuery3 query3 = new FlinkControllerQuery3();

        Thread thread_kafka_injection = new Thread(() -> {
            try {
                kc.kafkaStart();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread thread_query3= new Thread(() -> {
            try {
                query3.calculateQuery3();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        thread_kafka_injection.start();
        thread_query3.start();

        thread_kafka_injection.join();
        thread_query3.join();

    }

}