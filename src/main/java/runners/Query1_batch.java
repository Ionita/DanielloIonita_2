package runners;

import controllers.query1.batch.FlinkBatch1;

public class Query1_batch {

    public static void main(String[] args) {
        query1();
    }

    private static void query1() {
        FlinkBatch1 fc = new FlinkBatch1();
        try {
            fc.calculateAvg();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
