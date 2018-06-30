package controllers.query1;

import com.google.gson.Gson;
import entities.Friend;
import entities.Message;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

public class FlinkController implements Serializable {

    private static String INPUT_KAFKA_TOPIC = null;
    private Integer currentHour = -1;
    private Date accumulatorDate = null;

    public void calculateAvg() throws Exception {

        INPUT_KAFKA_TOPIC = "query1";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", INPUT_KAFKA_TOPIC);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer011(INPUT_KAFKA_TOPIC, new SimpleStringSchema(), properties));


        //System.out.println("got sources");
        DataStream<Tuple5<Integer,Integer, Date, Long, Long>> streamTuples = stream.flatMap(new Message2Tuple());

/*        SingleOutputStreamOperator<Tuple4<Integer, Integer, Date, Long>> resultStream =
                streamTuples
                .keyBy(1)
                .window(GlobalWindows.create())
                .trigger(new MyTrigger())
                .aggregate(new AverageAggregate());*/

        SingleOutputStreamOperator<Tuple4<Integer, Integer, Date, Long>> resultStream =
                streamTuples
                        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple5<Integer,Integer, Date, Long, Long>>(Time.hours(23)) {
                            @Override
                            public long extractTimestamp(Tuple5<Integer,Integer, Date, Long, Long> element) {
                                return element.f2.getTime();
                            }
                        })
                        .keyBy(1)
                        //.window(GlobalWindows.create())
                        //.trigger(new MyTrigger())
                        .timeWindow(Time.minutes(60))
                        .aggregate(new AverageAggregate());

        resultStream.addSink(new FlinkKafkaProducer011<>("localhost:9092", "monitor",  st -> {
            Message m = new Message(0);
            m.setTmp(String.valueOf(st.f2.getTime()));
            m.setHour(st.f0);
            m.setYear(st.f1);
            m.setCount(st.f3);
            return new Gson().toJson(m).getBytes();
        }));

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
            //System.out.println(accumulator.f2.toString());
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

    public static class Message2Tuple implements FlatMapFunction<String, Tuple5<Integer, Integer, Date, Long, Long>> {

        @Override
        public void flatMap(String jsonString, Collector<Tuple5<Integer, Integer, Date, Long, Long>> out) {
            ArrayList<Friend> recs = DataReader.getData(jsonString);
            Iterator irecs = recs.iterator();

            while (irecs.hasNext()) {
                Friend record = (Friend) irecs.next();
                Tuple5 tp5 = new Tuple5<>(record.getHour(), record.getYear(), record.getTmp(), record.getUser_1(), record.getUser_2());

                out.collect(tp5);
            }
        }
    }

    private class MyTrigger extends Trigger<Tuple5<Integer, Integer, Date, Long, Long>, GlobalWindow> {
        @Override
        public TriggerResult onElement(Tuple5<Integer, Integer, Date, Long, Long> tuple, long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {

            if (currentHour == -1)
                currentHour = tuple.f0;

            if (tuple.f0.equals(currentHour))
                return TriggerResult.CONTINUE;

            else {
                currentHour = tuple.f0;
                return TriggerResult.FIRE_AND_PURGE;
            }

        }

        @Override
        public TriggerResult onProcessingTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {

        }
    }
}