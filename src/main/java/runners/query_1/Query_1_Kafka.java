package runners.query_1;

import dataInjection.KafkaController;

public class Query_1_Kafka {

    public static void main(String[] args) {

        if(args.length == 1) {
            KafkaController kc = new KafkaController(1);
            try {
                kc.kafkaStart(args[0]);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }



    }

}
