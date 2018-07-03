package runners;

import controllers.query3.batch.FlinkFile3;

public class Query3_batch {

    public static void main(String[] args) {
        query3();
    }


    private static void query3() {

        FlinkFile3 query3 = new FlinkFile3();
        try {
            query3.calculateQuery3();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
