package runners.query_1;

import controllers.query1.fromFile.FlinkReadFromFile;

public class Query1_fromFile {

    public static void main(String[] args) {
        if(args.length == 1) {
            query1(args[0]);
        }
    }

    private static void query1(String arg) {
        FlinkReadFromFile fc = new FlinkReadFromFile(arg);
        try {
            fc.calculateAvg();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
