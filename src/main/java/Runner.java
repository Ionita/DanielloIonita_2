import controllers.FlinkController;
import controllers.FlinkControllerQuery2;
import dataInjection.KafkaController;
import controllers.Monitor;


public class Runner {

    public static void main(String[] args){
        KafkaController kc = new KafkaController();
        FlinkController fc = new FlinkController();
        FlinkControllerQuery2 query2 = new FlinkControllerQuery2();

        Thread thread_query1 = new Thread(() -> {
            try {
                fc.calculateAvg();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


        Thread thread_kafka_injection = new Thread(() -> {
            try {
                kc.kafkaStart();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread thread_monitor_query1 = new Thread(Monitor::new);


            Thread thread_query2= new Thread(() -> {
            try {
                query2.calculateQuery2();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        //thread_monitor_query1.start();
        //thread_query1.start();

        thread_kafka_injection.start();

        thread_query2.start();

        try {
            //thread_query1.join();
            //thread_monitor_query1.join();

            thread_kafka_injection.join();

            thread_query2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}