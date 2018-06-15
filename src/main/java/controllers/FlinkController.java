package controllers;

import com.google.gson.Gson;
import entities.Friend;
import entities.Message;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

public class FlinkController {

    private static String INPUT_KAFKA_TOPIC = null;

    public void calculateAvg() throws Exception {

        INPUT_KAFKA_TOPIC = "test";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", INPUT_KAFKA_TOPIC);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer011(INPUT_KAFKA_TOPIC, new SimpleStringSchema(), properties));


        System.out.println("got sources");
        DataStream<Tuple3<Integer, Long, Long>> streamTuples = stream.flatMap(new Message2Tuple());

        SingleOutputStreamOperator<Double> averageSpeedStream = streamTuples
                .keyBy(0)
                .timeWindow(Time.seconds((long)10))
                .aggregate(new AverageAggregate());



        averageSpeedStream.addSink(new FlinkKafkaProducer011<>("localhost:9092", "monitor",  stringDoubleTuple3 -> {
            System.out.println("ecco il valore: " + stringDoubleTuple3 + "\n\n");
            return new Gson().toJson(new Message(0)).getBytes();
        }));

        env.execute("Window Traffic Data");

    }

    private static class AverageAggregate implements AggregateFunction<Tuple3<Integer, Long, Long>, Tuple2<Long, Long>, Double> {

        @Override
        public Tuple2<Long, Long> createAccumulator() {
            Long l1 = 0L;
            Long l2 = 0L;
            return new Tuple2<>(l1, l2);
        }

        @Override
        public Tuple2<Long, Long> add(Tuple3<Integer, Long, Long> value, Tuple2<Long, Long> accumulator) {
            return new Tuple2<>(accumulator.f0, accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            //return accumulator.f0 / accumulator.f1;        }
            return accumulator.f1.doubleValue();
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }


    public static class Message2Tuple implements FlatMapFunction<String, Tuple3<Integer,Long,Long>> {

        @Override
        public void flatMap(String jsonString, Collector<Tuple3<Integer, Long, Long>> out) {
            ArrayList<Friend> recs = DataReader.getData(jsonString);
            Iterator irecs = recs.iterator();

            while (irecs.hasNext()) {
                Friend record = (Friend) irecs.next();
                Tuple3 tp3 = new Tuple3<>(record.getHour(), record.getUser_1(), record.getUser_2());

                out.collect(tp3);
            }
        }
    }
}