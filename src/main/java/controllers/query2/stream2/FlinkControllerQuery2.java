package controllers.query2.stream2;

import com.google.gson.Gson;
import entities.Comment;
import entities.Message;
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
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class FlinkControllerQuery2 implements Serializable {



    private static String INPUT_KAFKA_TOPIC = null;
    private static Integer currentHour = -1;
    private static ReentrantLock lock = new ReentrantLock();

    public void calculateQuery2() throws Exception {

        INPUT_KAFKA_TOPIC = "query2";
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
        DataStream<Tuple3<Date, Integer, Long>> streamTuples = stream.flatMap(new Message2Tuple());

        //streamTuples.print();
        SingleOutputStreamOperator<Tuple3<Date, Long, Long>> resultStream =
                streamTuples
                        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Date, Integer, Long>>(Time.hours(23)) {
                            @Override
                            public long extractTimestamp(Tuple3<Date, Integer, Long> element) {
                                return element.f0.getTime();
                            }
                        })
                        .keyBy(2)
                        .timeWindow(Time.minutes(60))
                        .aggregate(new AverageAggregate())
                        .setParallelism(1);

        resultStream.addSink(new FlinkKafkaProducer011<>("localhost:9092", "monitor_query2",  st -> {
            Message m = new Message(2);
            m.setTmp(String.valueOf(st.f0.getTime()));
            m.setPost_commented(st.f1);
            m.setCount(st.f2);
            //System.out.println("invio con tmp: " + m.getTmp());
            return new Gson().toJson(m).getBytes();
        }));

        env.execute("Query 2 Real-Time Classification");

    }

    private static class AverageAggregate implements AggregateFunction<Tuple3<Date, Integer, Long>, Tuple3<Date, Long, Long>, Tuple3<Date, Long, Long>> {

        @Override
        public Tuple3<Date, Long, Long> createAccumulator() {
            return new Tuple3<>(null, 0L, 0L);
        }

        @Override
        public Tuple3<Date, Long, Long> add(Tuple3<Date, Integer, Long> value, Tuple3<Date, Long, Long> accumulator) {
            if (accumulator.f0 == null)
                return new Tuple3<>(value.f0, value.f2, accumulator.f2 + 1);
            else if (accumulator.f0.after(value.f0))
                return new Tuple3<>(value.f0, value.f2, accumulator.f2 + 1);
            else if (accumulator.f0.before(value.f0))
                return new Tuple3<>(accumulator.f0, value.f2, accumulator.f2 + 1);
            else
                return new Tuple3<>(value.f0, value.f2, accumulator.f2 + 1);
        }

        @Override
        public Tuple3<Date, Long, Long> getResult(Tuple3<Date, Long, Long> accumulator) {
            //System.out.println("accumulator: " + accumulator.f0.toString());
            return accumulator;
        }

        @Override
        public Tuple3<Date, Long, Long> merge(Tuple3<Date, Long, Long> a, Tuple3<Date, Long, Long> b) {
            if(a.f0.before(b.f0)) {
                return a;
            }
            else {
                return b;
            }
        }
    }

    public static class Message2Tuple implements FlatMapFunction<String, Tuple3<Date, Integer, Long>> {

        @Override
        public void flatMap(String jsonString, Collector<Tuple3<Date, Integer, Long>> out) {
            ArrayList<Comment> recs = DataReaderQuery2.getData(jsonString);
            Iterator irecs = recs.iterator();

            while (irecs.hasNext()) {
                Comment record = (Comment) irecs.next();
                Tuple3 tp3 = new Tuple3<>(record.getTmp(), record.getHour(), record.getPost_commented());

                out.collect(tp3);
            }
        }
    }

    private class MyTrigger extends Trigger<Tuple3<Date, Integer, Long>, GlobalWindow> {
        @Override
        public TriggerResult onElement(Tuple3<Date, Integer, Long> tuple, long l, GlobalWindow globalWindow, TriggerContext triggerContext) {

            if (currentHour == -1) {
                lock.lock();
                if (currentHour == -1) {
                    currentHour = tuple.f1;
                    System.out.println("first tuple: " + tuple.toString());
                    return TriggerResult.FIRE_AND_PURGE;
                }
                lock.unlock();
            }

            if (tuple.f1 >= currentHour) {
                if(tuple.f1 > currentHour) {
                    currentHour = tuple.f1;
                    return TriggerResult.FIRE_AND_PURGE;
                }
                return TriggerResult.CONTINUE;
            }
            else {
                return TriggerResult.FIRE_AND_PURGE;
            }

        }

        @Override
        public TriggerResult onProcessingTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow globalWindow, TriggerContext triggerContext) {

        }
    }
}
