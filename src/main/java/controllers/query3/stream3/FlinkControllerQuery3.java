package controllers.query3.stream3;

import com.google.gson.Gson;
import entities.Message;
import entities.TopUsers;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

public class FlinkControllerQuery3 implements Serializable {



    private static String INPUT_KAFKA_TOPIC = null;
    private Integer currentHour = -1;
    private Date accumulatorDate = null;

    public void calculateQuery3() throws Exception {
        INPUT_KAFKA_TOPIC = "query3";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        String randomId = UUID.randomUUID().toString();
        properties.setProperty("group.id", randomId);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer011(INPUT_KAFKA_TOPIC, new SimpleStringSchema(), properties));

        env.setParallelism(1);


        //System.out.println("got sources");
        DataStream<Tuple4<Date, Integer, Long, String>> streamTuples = stream.flatMap(new Message2Tuple());

        SingleOutputStreamOperator<Tuple4<Date, Long, Long, String>> resultStream =
                streamTuples
                        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple4<Date, Integer, Long, String>>(Time.minutes(60)) {
                    @Override
                    public long extractTimestamp(Tuple4<Date, Integer, Long, String> element) {
                        return element.f0.getTime();
                    }
                })
                        .keyBy(2)
                        //.window(GlobalWindows.create())
                        //.trigger(new MyTrigger())
                        .timeWindow(Time.minutes(60))
                        .aggregate(new AverageAggregate())
                        .setParallelism(1);

        resultStream.addSink(new FlinkKafkaProducer011<>("localhost:9092", "monitor_query3",  st -> {
            Message m = new Message(3);
            m.setTmp(String.valueOf(st.f0.getTime()));
            m.setUser_id1(st.f1);
            m.setCount(st.f2);
            m.setUser_name(st.f3);
            return new Gson().toJson(m).getBytes();
        }));

        env.execute("Query 3 Real-Time Classification");

    }


    //TODO: change how is taken id value, is wrong
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

    public static class Message2Tuple implements FlatMapFunction<String, Tuple4<Date, Integer, Long, String>> {

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

    /*private class MyTrigger extends Trigger<Tuple4<Date, Integer, Long, String>, GlobalWindow> {
        @Override
        public TriggerResult onElement(Tuple4<Date, Integer, Long, String> tuple, long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {

            if (currentHour == -1)
                currentHour = tuple.f1;

            if (tuple.f1.equals(currentHour))
                return TriggerResult.CONTINUE;

            else {
                currentHour = tuple.f1;
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
    }*/
}