package runners;

import controllers.query1.batch.FlinkBatch1;

public class Query1_batch {

    public static void main(String[] args) {
        if(args.length == 1) {
            query1(args[0]);
        }
    }

    private static void query1(String arg) {
        FlinkBatch1 fc = new FlinkBatch1(arg);
        try {
            fc.calculateAvg();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
