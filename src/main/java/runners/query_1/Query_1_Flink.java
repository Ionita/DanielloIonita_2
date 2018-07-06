package runners.query_1;

import controllers.query1.stream.FlinkController;

public class Query_1_Flink {

    public static void main(String[] args) {
        FlinkController fc = new FlinkController();
        try {
            fc.startComputation();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
