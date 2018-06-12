import controllers.KafkaController;

import java.io.IOException;

public class Runner {

    public static void main(String[] args) throws IOException {
        KafkaController kc = new KafkaController();
        kc.readData();
    }
}
