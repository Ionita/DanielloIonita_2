package runners.query_3;


import controllers.query3.stream3.FlinkControllerQuery3;

public class Query_3_Flink {

    public static void main(String[] args) {
        FlinkControllerQuery3 fc = new FlinkControllerQuery3();
        try {
            fc.calculateQuery3();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
