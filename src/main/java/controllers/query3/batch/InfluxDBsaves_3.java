package controllers.query3.batch;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class InfluxDBsaves_3 {

    private static boolean is_working = true;

    private static InfluxDBsaves_3 instance = new InfluxDBsaves_3();
    private static InfluxDB influxDB;
    private static BatchPoints batchPoints;

    private static final String dbname = "query3";

    public static InfluxDBsaves_3 getInstance(){
        return instance;
    }

    private InfluxDBsaves_3(){

        try {
            influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
            influxDB.setDatabase(dbname);
            influxDB.enableBatch(BatchOptions.DEFAULTS);
            batchPoints = createBatch();
        }
        catch (Exception e) {
            is_working = false;
        }
    }

    public void savePointOnDB(String table, Date ts, Integer[] value){
        if (is_working) {
            Point point = Point.measurement(table)
                    .time(ts.getTime(), TimeUnit.MILLISECONDS)
                    .addField("user_0", value[0])
                    .addField("count_0", value[1])
                    .addField("user_1", value[2])
                    .addField("count_1", value[3])
                    .addField("user_2", value[4])
                    .addField("count_2", value[5])
                    .addField("user_3", value[6])
                    .addField("count_3", value[7])
                    .addField("user_4", value[8])
                    .addField("count_4", value[9])
                    .addField("user_5", value[10])
                    .addField("count_5", value[11])
                    .addField("user_6", value[12])
                    .addField("count_6", value[13])
                    .addField("user_7", value[14])
                    .addField("count_7", value[15])
                    .addField("user_8", value[16])
                    .addField("count_8", value[17])
                    .addField("user_9", value[18])
                    .addField("count_9", value[19])
                    .build();
            batchPoints.point(point);
            influxDB.write(batchPoints);
        }
    }

    private static BatchPoints createBatch(){
        return BatchPoints
                .database(dbname)
                .tag("async", "true")
                .consistency(InfluxDB.ConsistencyLevel.ALL)
                .build();
    }


    private static boolean testConnection(){
        Pong response = influxDB.ping();
        if(response.getVersion().equalsIgnoreCase("unknown")){
            System.out.println("InfluxDB not found");
            return false;
        }
        else
            return true;
    }

}
