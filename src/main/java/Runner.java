import controllers.KafkaController;

public class Runner {

    public static void main(String[] args){
        KafkaController kc = new KafkaController();




        Thread thread2 = new Thread(() -> {
            try {
                kc.kafkaStart();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        thread2.start();

    }
}
