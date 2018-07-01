package controllers.query2.stream_batch;

import com.sun.org.apache.xpath.internal.operations.Bool;
import entities.Message;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
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
import java.util.concurrent.locks.ReentrantLock;

public class FlinkFile {


    private final static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private static ReentrantLock lock = new ReentrantLock();
    private static ReentrantLock lockCalendar = new ReentrantLock();
    private static ReentrantLock lockArray = new ReentrantLock();

    private static int counterToDelete = 0;
    private static int minimumHourSize = 4;
    private static boolean isIn = false;

    private static ArrayList<Long> minimumHour = new ArrayList<>();



    public void calculateQuery2() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        String randomId = UUID.randomUUID().toString();
        properties.setProperty("group.id", randomId);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream =
                env.readTextFile("/home/simone/IdeaProjects/DanielloIonita_2/data/comments.dat");

        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<Date, Integer, Long>> streamTuples =
                stream.flatMap(new Tokenizer());


        streamTuples
            .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple3<Date, Integer, Long>>() {
                @Override
                public long extractTimestamp(Tuple3<Date, Integer, Long> element, long l) {
                    return element.f0.getTime();
                }

                @Override
                public Watermark checkAndGetNextWatermark(Tuple3<Date, Integer, Long> element, long l) {
                    return new Watermark(element.f0.getTime() - 1);
                }
            })
            .keyBy(2)
            .timeWindow(Time.hours(1))
            .aggregate(new AverageAggregate());


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

            //Long currentTmp = accumulator.f0.getTime();

            Message messageMonitor = new Message();
            messageMonitor.setTmp(String.valueOf(accumulator.f0.getTime()));
            messageMonitor.setPost_commented(accumulator.f1);
            messageMonitor.setCount(accumulator.f2);

            //lockArray.lock();
            minimumHour.add(accumulator.f0.getTime());
            //lockArray.unlock();

            //while(!checkIfValueIsMinimum(currentTmp)){}
            //isIn = true;
            MonitorBatch2.getInstance().makeCheck(messageMonitor);
            //isIn = false;
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

    public static final class Tokenizer implements FlatMapFunction<String, Tuple3<Date, Integer, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple3<Date, Integer, Long>> out) throws Exception {
            String[] bufferReading = value.split("\\|");

            if (bufferReading[5].equals("") || bufferReading[5].equals(" ")) {
                SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
                Date timestamp = sdf.parse(bufferReading[0]);
                Calendar c = GregorianCalendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"));
                c.setTime(timestamp);

                Integer hour = (c.get(Calendar.HOUR_OF_DAY));
                Long postCommented = Long.valueOf(bufferReading[6]);

                out.collect(new Tuple3<>(timestamp, hour, postCommented));
            }

        }
    }

/*
    private static boolean checkIfValueIsMinimum(Long currentDate){

        lockArray.lock();
        if(isIn){
            lockArray.unlock();
            return false;
        }
        if(minimumHour.size() > minimumHourSize)
            minimumHourSize = minimumHour.size();
        if(minimumHour.size() < minimumHourSize) {
            counterToDelete ++;
            if(counterToDelete % 1000000 == 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //System.out.println("esco qui " + currentDate + ": " + minimumHour.toString());
                //System.out.println("setto la finestra a " + minimumHour.size());
                minimumHourSize = minimumHour.size();
                counterToDelete = 0;
            }
            lockArray.unlock();
            return false;
        }
        for(Long d : minimumHour){
            if (currentDate > d) {
                lockArray.unlock();
                return false;
            }
        }
        minimumHour.remove(currentDate);
        lockArray.unlock();
        return true;
    }
*/


}
