package runners;

import controllers.query2.stream_batch.FlinkFile;

public class Query2_batch {

    public static void main(String[] args) {
        query2();
    }


    private static void query2() {

        FlinkFile query2 = new FlinkFile();
        try {
            query2.calculateQuery2();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
