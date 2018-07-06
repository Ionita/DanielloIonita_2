package runners.query_2;

import dataInjection.KafkaController;

public class Query_2_Kafka {

    public static void main(String[] args) {

        if(args.length == 1) {
            KafkaController kc = new KafkaController(2);
            try {
                kc.kafkaStart(args[0]);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }



    }

}
