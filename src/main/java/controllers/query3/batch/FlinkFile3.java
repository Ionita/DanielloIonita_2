package controllers.query3.batch;

import controllers.query2.stream_batch.MonitorBatch2_light;
import controllers.query3.stream.DataReaderQuery3;
import entities.Message;
import entities.TopUsers;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

public class FlinkFile3 {


    private final static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    /*private static String INPUT_KAFKA_TOPIC = null;
    private static Integer currentHour = -1;
    private static ReentrantLock lock = new ReentrantLock();
    private static int countToDelete = 0;*/


    public void calculateQuery3() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream =
                env.readTextFile("/Users/mariusdragosionita/Documents/workspace/DanielloIonita_2/data/comments.dat");

        SingleOutputStreamOperator<Tuple4<Date, Integer, Long, String>> streamTuples =
                stream.flatMap(new Tokenizer());


        //SingleOutputStreamOperator<Tuple4<Date, Long, Long, String>> resultStream =
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
        public void flatMap(String jsonString, Collector<Tuple4<Date, Integer, Long, String>> out) {
            ArrayList<TopUsers> recs = DataReaderQuery3.getData(jsonString);
            Iterator irecs = recs.iterator();

            while (irecs.hasNext()) {
                TopUsers record = (TopUsers) irecs.next();
                Tuple4 tp4 = new Tuple4<>(record.getTmp(), record.getHour(), record.getUser_id(), record.getUsername());

                out.collect(tp4);
            }
        }
    }
}
