package controllers.query2;

import com.google.gson.Gson;
import entities.Comment;
import entities.Message;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FlinkFile {


    private final static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

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
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> stream = env.readTextFile("file:///Users/mariusdragosionita/Documents/workspace/DanielloIonita_2/data/comments.dat");


        //System.out.println("got sources");
        DataSet<Tuple4<Date, Integer, Long, Integer>> streamTuples = stream.flatMap(new Message2Tuple());

        AggregateOperator<Tuple4<Date, Integer, Long, Integer>> resultStream =
                streamTuples
                        .groupBy(2)
                        .sum(3);
                        //.window(GlobalWindows.create())
                        //.trigger(new MyTrigger())
                        //.aggregate(new AverageAggregate());

        resultStream.print();

        /*resultStream.addSink(new FlinkKafkaProducer011<>("localhost:9092", "monitor_query2",  st -> {
            Message m = new Message(2);
            m.setTmp(String.valueOf(st.f0.getTime()));
            m.setPost_commented(st.f1);
            m.setCount(st.f2);
            //System.out.println("invio con tmp: " + m.getTmp());
            return new Gson().toJson(m).getBytes();
        }));*/

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

    public static class Message2Tuple implements FlatMapFunction<String, Tuple4<Date, Integer, Long, Integer>> {

        @Override
        public void flatMap(String jsonString, Collector<Tuple4<Date, Integer, Long, Integer>> out) {
            ArrayList<Comment> recs = newDataReaderQuery2(jsonString);
            Iterator irecs = recs.iterator();

            while (irecs.hasNext()) {
                Comment record = (Comment) irecs.next();
                Tuple4 tp3 = new Tuple4<>(record.getTmp(), record.getHour(), record.getPost_commented(), 1);

                out.collect(tp3);
            }
        }
    }

    private static ArrayList<Comment>  newDataReaderQuery2 (String line) {

        int i = 0;
        int j = 1;
        String cvsSplitBy = "\\|";
        ArrayList<Comment> comments = new ArrayList<>();

                String[] bufferReading = line.split(cvsSplitBy, -1);

                // use comma as separator
                Comment m = new Comment();
        try {
            m.setTmp(new SimpleDateFormat(dateFormat).parse(bufferReading[0]));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        m.setComment_id(Long.valueOf(bufferReading[1]));
                m.setUser_id(Long.valueOf(bufferReading[2]));
                m.setComment(bufferReading[3]);
                m.setUser_name(bufferReading[4]);
                if (bufferReading[5].equals("") || bufferReading[5].equals(" ")) {
                    m.setComment_replied(null);
                    m.setPost_commented(Long.valueOf(bufferReading[6]));
                    comments.add(m);
                }

        return  comments;
    }


}
