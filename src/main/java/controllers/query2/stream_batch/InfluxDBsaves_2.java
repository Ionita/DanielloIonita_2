package controllers.query2.stream_batch;

import org.influxdb.InfluxDB;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class InfluxDBsaves_2 {

    private static boolean is_working = true;

    private static InfluxDBsaves_2 instance = new InfluxDBsaves_2();
    private static InfluxDB influxDB;
    private static BatchPoints batchPoints;

    private static final String dbname = "query2";

    public static InfluxDBsaves_2 getInstance(){
        return instance;
    }

    private InfluxDBsaves_2(){
        try {
            influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
            influxDB.setDatabase(dbname);
            influxDB.enableBatch(BatchOptions.DEFAULTS);
            batchPoints = createBatch();
        }
        catch (Exception e){
            System.out.println("InfluxDB not working");
            is_working = false;
        }
    }

    public void savePointOnDB(String table, Date ts, Integer[] value){
        if (is_working) {
            Point point = Point.measurement(table)
                    .time(ts.getTime(), TimeUnit.MILLISECONDS)
                    .addField("post_0", value[0])
                    .addField("count_0", value[1])
                    .addField("post_1", value[2])
                    .addField("count_1", value[3])
                    .addField("post_2", value[4])
                    .addField("count_2", value[5])
                    .addField("post_3", value[6])
                    .addField("count_3", value[7])
                    .addField("post_4", value[8])
                    .addField("count_4", value[9])
                    .addField("post_5", value[10])
                    .addField("count_5", value[11])
                    .addField("post_6", value[12])
                    .addField("count_6", value[13])
                    .addField("post_7", value[14])
                    .addField("count_7", value[15])
                    .addField("post_8", value[16])
                    .addField("count_8", value[17])
                    .addField("post_9", value[18])
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
