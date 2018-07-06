package controllers.query3.fromFile3;

import entities.Message;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class FlinkFromFile3 {


    private final static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private String filepath;

    public FlinkFromFile3(String arg) {
        filepath = arg;
    }


    public void calculateQuery3() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream =
                env.readTextFile(filepath);

        SingleOutputStreamOperator<Tuple4<Date, Integer, Long, String>> streamTuples =
                stream.flatMap(new Tokenizer());


        streamTuples
            .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple4<Date, Integer, Long, String>>() {
                @Override
                public long extractTimestamp(Tuple4<Date, Integer, Long, String> element, long l) {
                    return element.f0.getTime();
                }

                @Override
                public Watermark checkAndGetNextWatermark(Tuple4<Date, Integer, Long, String> element, long l) {
                    return new Watermark(element.f0.getTime() - 1);
                }
            })
            .keyBy(2)
            .timeWindow(Time.hours(1))
            .aggregate(new AverageAggregate())
            .setParallelism(1);
        env.execute("Query 3 Real-Time Classification");

    }

    private static class AverageAggregate implements AggregateFunction<Tuple4<Date, Integer, Long, String>, Tuple4<Date, Long, Long, String>, Tuple4<Date, Long, Long, String>> {

        @Override
        public Tuple4<Date, Long, Long, String> createAccumulator() {
            return new Tuple4<>(null, 0L, 0L, null);
        }

        @Override
        public Tuple4<Date, Long, Long, String> add(Tuple4<Date, Integer, Long, String> value, Tuple4<Date, Long, Long, String> accumulator) {
            if (accumulator.f0 == null)
                return new Tuple4<>(value.f0, value.f2, accumulator.f2 + 1, value.f3);
            else if (accumulator.f0.after(value.f0))
                return new Tuple4<>(value.f0, value.f2, accumulator.f2 + 1, value.f3);
            else if (accumulator.f0.before(value.f0))
                return new Tuple4<>(accumulator.f0, value.f2, accumulator.f2 + 1, value.f3);
            else
                return new Tuple4<>(value.f0, value.f2, accumulator.f2 + 1, value.f3);
        }

        @Override
        public Tuple4<Date, Long, Long, String> getResult(Tuple4<Date, Long, Long, String> accumulator) {
            Message m = new Message();
            m.setTmp(String.valueOf(accumulator.f0.getTime()));
            m.setUser_id1(accumulator.f1);
            m.setCount(accumulator.f2);
            m.setUser_name(accumulator.f3);
            MonitorFromFile3.getInstance().makeCheck(m);
            return accumulator;
        }

        @Override
        public Tuple4<Date, Long, Long, String> merge(Tuple4<Date, Long, Long, String> a, Tuple4<Date, Long, Long, String> b) {
            if(a.f0.before(b.f0)) {
                return a;
            }
            else {
                return b;
            }
        }
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple4<Date, Integer, Long, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple4<Date, Integer, Long, String>> out) {
            String[] bufferReading = value.split("\\|");
            Tuple4 tp4;
            Tuple4 tp4_2;
            Date timestamp;
            Integer hour;
            String username;
            Long user1;
            Long user2;

            try {

                if (bufferReading.length == 3){
                    //is friendship
                    timestamp = new SimpleDateFormat(dateFormat).parse(bufferReading[0]);
                    Calendar c = GregorianCalendar.getInstance();
                    c.setTime(timestamp);
                    hour = c.get(Calendar.HOUR_OF_DAY);
                    user1 = Long.parseLong(bufferReading[1]);
                    user2 = Long.parseLong(bufferReading[2]);
                    tp4 = new Tuple4<>(timestamp, hour, user1, "null");
                    tp4_2 = new Tuple4<>(timestamp, hour, user2, "null");

                    out.collect(tp4);
                    out.collect(tp4_2);
                }
                else if(bufferReading.length == 7 || bufferReading.length == 6){
                    //is comment
                    timestamp = new SimpleDateFormat(dateFormat).parse(bufferReading[0]);
                    Calendar c = GregorianCalendar.getInstance();
                    c.setTime(timestamp);
                    hour = c.get(Calendar.HOUR_OF_DAY);
                    user1 = Long.valueOf(bufferReading[2]);
                    username = bufferReading[4];
                    tp4 = new Tuple4<>(timestamp, hour, user1, username);

                    out.collect(tp4);
                }
                else if(bufferReading.length == 5){
                    //is post
                    timestamp = new SimpleDateFormat(dateFormat).parse(bufferReading[0]);
                    Calendar c = GregorianCalendar.getInstance();
                    c.setTime(timestamp);
                    hour = c.get(Calendar.HOUR_OF_DAY);
                    user1 = Long.valueOf(bufferReading[2]);
                    username = bufferReading[4];
                    tp4 = new Tuple4<>(timestamp, hour, user1, username);

                    out.collect(tp4);
                }
                else{
                    //error
                    System.out.println("error");
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}
