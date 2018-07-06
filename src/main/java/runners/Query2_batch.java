package runners;

import controllers.query2.stream_batch.FlinkFile;

public class Query2_batch {

    public static void main(String[] args) {
        if(args.length == 1)
            query2(args[0]);
    }


    private static void query2(String arg) {

        FlinkFile query2 = new FlinkFile(arg);
        try {
            query2.calculateQuery2();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
