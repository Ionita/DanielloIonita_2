import controllers.FlinkController;
import dataInjection.KafkaController;
import controllers.Monitor;


public class Runner {

    public static void main(String[] args){
        KafkaController kc = new KafkaController();
        FlinkController fc = new FlinkController();

        Thread thread1 = new Thread(() -> {
            try {
                fc.calculateAvg();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


        Thread thread2 = new Thread(() -> {
            try {
                kc.kafkaStart();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread thread3 = new Thread(Monitor::new);

        thread3.start();
        thread1.start();
        thread2.start();

        try {
            thread1.join();
            thread2.join();
            thread3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}