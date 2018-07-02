package controllers.query1.batch;

import com.google.gson.Gson;
import controllers.query1.stream.DataReader;
import entities.Friend;
import entities.Message;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public class FlinkBatch1 implements Serializable {

    private static String INPUT_KAFKA_TOPIC = null;
    private final static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    private Integer currentHour = -1;
    private Date accumulatorDate = null;

    public void calculateAvg() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> stream =
                env.readTextFile("/home/simone/IdeaProjects/DanielloIonita_2/data/friendships.dat");

        env.setParallelism(1);
        //System.out.println("got sources");
        DataStream<Tuple5<Integer,Integer, Date, Long, Long>> streamTuples = stream.flatMap(new Tokenizer());


        streamTuples
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple5<Integer,Integer, Date, Long, Long>>(Time.hours(23)) {
                @Override
                public long extractTimestamp(Tuple5<Integer,Integer, Date, Long, Long> element) {
                    return element.f2.getTime();
                }
            })
            .keyBy(1)
            .timeWindow(Time.minutes(60))
            .aggregate(new AverageAggregate());

        env.execute("Window Traffic Data");

    }

    private static class AverageAggregate implements AggregateFunction<Tuple5<Integer, Integer, Date, Long, Long>, Tuple4<Integer, Integer, Date, Long>, Tuple4<Integer, Integer, Date, Long>> {

        @Override
        public Tuple4<Integer, Integer, Date, Long> createAccumulator() {
            Long l1 = 0L;
            return new Tuple4<>(0, 0, null, l1);
        }

        @Override
        public Tuple4<Integer, Integer, Date, Long> add(Tuple5<Integer, Integer, Date, Long, Long> value, Tuple4<Integer, Integer, Date, Long> accumulator) {
            if (accumulator.f2 == null)
                return new Tuple4<>(value.f0, value.f1, value.f2, accumulator.f3 + 1);
            else if (accumulator.f2.after(value.f2))
                return new Tuple4<>(value.f0, value.f1, value.f2, accumulator.f3 + 1);
            else if (accumulator.f2.before(value.f2))
                return new Tuple4<>(value.f0, value.f1, accumulator.f2, accumulator.f3 + 1);
            else
                return new Tuple4<>(value.f0, value.f1, value.f2, accumulator.f3 + 1);
        }

        @Override
        public Tuple4<Integer, Integer, Date, Long> getResult(Tuple4<Integer, Integer, Date, Long> accumulator) {
            Message m = new Message(0);
            m.setTmp(String.valueOf(accumulator.f2.getTime()));
            m.setHour(accumulator.f0);
            m.setYear(accumulator.f1);
            m.setCount(accumulator.f3);
            MonitorBatch1.getInstance().makeCheck(m);
            return accumulator;
        }

        @Override
        public Tuple4<Integer, Integer, Date, Long> merge(Tuple4<Integer, Integer, Date, Long> a, Tuple4<Integer, Integer, Date, Long> b) {
            if(a.f2.before(b.f2)) {
                return a;
            }
            else {
                return b;
            }
        }
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple5<Integer, Integer, Date, Long, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple5<Integer, Integer, Date, Long, Long>> out) throws Exception {
            String[] bufferReading = value.split("\\|");

            Long user1 = Long.valueOf(bufferReading[1]);
            Long user2 = Long.valueOf(bufferReading[2]);
            Date timestamp = new SimpleDateFormat(dateFormat).parse(bufferReading[0]);
            Calendar c = GregorianCalendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"));
            c.setTime(timestamp);
            Integer hour = (c.get(Calendar.HOUR_OF_DAY));
            Integer year = (c.get(Calendar.YEAR));

            out.collect(new Tuple5<>(hour, year, timestamp, user1, user2));

        }
    }




}