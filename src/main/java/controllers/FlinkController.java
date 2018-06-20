package controllers;

import com.google.gson.Gson;
import entities.Friend;
import entities.Message;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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


        //System.out.println("got sources");
        DataStream<Tuple6<Integer, Integer, Integer, Integer, Long, Long>> streamTuples = stream.flatMap(new Message2Tuple());

        SingleOutputStreamOperator<Tuple5<Integer, Integer, Integer, Integer, Long>> resultStream = streamTuples
                .keyBy(0, 1, 2, 3)
                .countWindow(1)
                .aggregate(new AverageAggregate());


        resultStream.addSink(new FlinkKafkaProducer011<>("localhost:9092", "monitor2",  st -> {
            //System.out.println("stranger things");
            Message m = new Message(0);
            m.setHour(st.f0);
            m.setDay(st.f1);
            m.setWeek(st.f2);
            m.setYear(st.f3);
            m.setCount(st.f4);
            return new Gson().toJson(m).getBytes();
        }));

        env.execute("Window Traffic Data");
    }

    private static class AverageAggregate implements AggregateFunction<Tuple6<Integer, Integer, Integer, Integer, Long, Long>, Tuple5<Integer, Integer, Integer, Integer, Long>, Tuple5<Integer, Integer, Integer, Integer, Long>> {

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Long> createAccumulator() {
            Long l1 = 0L;
            return new Tuple5<>(0, 0, 0, 0, l1);
        }

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Long> add(Tuple6<Integer, Integer, Integer, Integer, Long, Long> value, Tuple5<Integer, Integer, Integer, Integer, Long> accumulator) {
            return new Tuple5<>(value.f0, value.f1, value.f2, value.f3, accumulator.f4 + 1);
        }

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Long> getResult(Tuple5<Integer, Integer, Integer, Integer, Long> accumulator) {
            //return accumulator.f0 / accumulator.f1;        }
            return accumulator;
        }

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Long> merge(Tuple5<Integer, Integer, Integer, Integer, Long> a, Tuple5<Integer, Integer, Integer, Integer, Long> b) {
            //return new Tuple4<Integer, Integer, Integer, Long> (a.f0 + b.f0, a.f1 + b.f1);
            return a;
        }
    }


    public static class Message2Tuple implements FlatMapFunction<String, Tuple6<Integer, Integer, Integer, Integer, Long, Long>> {

        @Override
        public void flatMap(String jsonString, Collector<Tuple6<Integer, Integer, Integer, Integer, Long, Long>> out) {
            ArrayList<Friend> recs = DataReader.getData(jsonString);
            Iterator irecs = recs.iterator();

            while (irecs.hasNext()) {
                Friend record = (Friend) irecs.next();
                Tuple6 tp6 = new Tuple6<>(record.getHour(), record.getDay(), record.getWeek(), record.getYear(), record.getUser_1(), record.getUser_2());

                out.collect(tp6);
            }
        }
    }

}