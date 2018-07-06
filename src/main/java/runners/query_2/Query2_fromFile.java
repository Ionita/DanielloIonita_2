package runners.query_2;

import controllers.query2.fromFile2.FlinkFile;
import org.apache.flink.api.java.utils.ParameterTool;

public class Query2_fromFile {

    public static void main(String[] args) {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        query2(parameter);
    }


    private static void query2(ParameterTool parameter) {


        FlinkFile query2 = new FlinkFile(parameter);
        try {
            query2.calculateQuery2();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
