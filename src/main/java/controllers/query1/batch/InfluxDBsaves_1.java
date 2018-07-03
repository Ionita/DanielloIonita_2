package controllers.query1.batch;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class InfluxDBsaves_1 {

    private static InfluxDBsaves_1 instance = new InfluxDBsaves_1();
    private static InfluxDB influxDB;
    private static BatchPoints batchPoints;

    private static final String dbname = "prova";

    public static InfluxDBsaves_1 getInstance(){
        return instance;
    }

    private InfluxDBsaves_1(){
        influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
        influxDB.setDatabase(dbname);
        influxDB.enableBatch(BatchOptions.DEFAULTS);
        batchPoints = createBatch();
    }

    public void savePointOnDB(String table, Date ts, Integer[] value){
        Point point = Point.measurement(table)
                .time(ts.getTime(), TimeUnit.MILLISECONDS)
                .addField("hour_0", value[0])
                .addField("hour_1", value[1])
                .addField("hour_2", value[2])
                .addField("hour_3", value[3])
                .addField("hour_4", value[4])
                .addField("hour_5", value[5])
                .addField("hour_6", value[6])
                .addField("hour_7", value[7])
                .addField("hour_8", value[8])
                .addField("hour_9", value[9])
                .addField("hour_10", value[10])
                .addField("hour_11", value[11])
                .addField("hour_12", value[12])
                .addField("hour_13", value[13])
                .addField("hour_14", value[14])
                .addField("hour_15", value[15])
                .addField("hour_16", value[16])
                .addField("hour_17", value[17])
                .addField("hour_18", value[18])
                .addField("hour_19", value[19])
                .addField("hour_20", value[20])
                .addField("hour_21", value[21])
                .addField("hour_22", value[22])
                .addField("hour_23", value[23])
                .build();
        batchPoints.point(point);
        influxDB.write(batchPoints);
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
