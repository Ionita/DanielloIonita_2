package runners;

import controllers.query3.batch.FlinkFile3;

public class Query3_batch {

    public static void main(String[] args) {
        if(args.length == 1)
            query3(args[0]);
    }


    private static void query3(String arg) {

        FlinkFile3 query3 = new FlinkFile3(arg);
        try {
            query3.calculateQuery3();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
