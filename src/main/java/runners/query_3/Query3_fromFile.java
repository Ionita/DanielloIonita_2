package runners.query_3;

import controllers.query3.fromFile3.FlinkFromFile3;

public class Query3_fromFile {

    public static void main(String[] args) {
        if(args.length == 1)
            query3(args[0]);
    }


    private static void query3(String arg) {

        FlinkFromFile3 query3 = new FlinkFromFile3(arg);
        try {
            query3.calculateQuery3();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
