package runners.query_3;

import dataInjection.KafkaController;

public class Query_3_Kafka {

    public static void main(String[] args) {

        if(args.length == 1) {
            KafkaController kc = new KafkaController(3);
            try {
                kc.kafkaStart(args[0]);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }



    }

}
