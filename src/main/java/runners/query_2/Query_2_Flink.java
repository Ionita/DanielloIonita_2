package runners.query_2;

import controllers.query2.stream2.FlinkControllerQuery2;

public class Query_2_Flink {

    public static void main(String[] args) {
        FlinkControllerQuery2 fc = new FlinkControllerQuery2();
        try {
            fc.calculateQuery2();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
