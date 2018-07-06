package runners.query_2;

import controllers.query2.fromFile2.FlinkFile;

public class Query2_fromFile {

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
