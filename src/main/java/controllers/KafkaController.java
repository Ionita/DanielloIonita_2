package controllers;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import entities.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.kafka.common.serialization.Serializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class KafkaController implements Serializer {

    private int i=0;
    private int j=1;
    final Producer<Long, String> producer;
    private final static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    public KafkaController(){
        producer = createProducer();
    }

    private final String BOOTSTRAP_SERVERS =
//            "localhost:9092,localhost:9093,localhost:9094";
            "localhost:9092";


    private Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }


    public void sendMessage(String address, Message m, String topic) {

        long time = System.currentTimeMillis();

        try {

//            Printer.getInstance().print("sent message with code: " + m.getCode(), "yellow");
            ObjectMapper mapper = new ObjectMapper();

            String toSend =  mapper.writeValueAsString(m);
            final ProducerRecord<Long, String> record =
                    new ProducerRecord<>(topic, time, toSend);

            producer.send(record).get();

        } catch (JsonProcessingException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
//            producer.flush();
//            producer.close();
        }
    }


    @Override
    public void configure(Map map, boolean b) {

    }


    @Override
    public byte[] serialize(String arg0, Object arg1) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(arg1).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    public void sendSemaphoreSensorInfo(String address, Object s, String topic) {

        try {

            ObjectMapper mapper = new ObjectMapper();

            String toSend =  mapper.writeValueAsString(s);
            System.out.println(toSend);
            final ProducerRecord<Long, String> record =
                    new ProducerRecord<>(topic, toSend);

            producer.send(record).get();

        } catch (JsonProcessingException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
//            producer.flush();
//            producer.close();
        }
    }



    @Override
    public void close() {

    }

    public void kafkaStart() {

        Thread thread1 = new Thread(() -> {
            readData("/Users/mariusdragosionita/Documents/workspace/DanielloIonita_2/data/friendships.dat", 0);
        });

        Thread thread2 = new Thread(() -> {
            readData("/Users/mariusdragosionita/Documents/workspace/DanielloIonita_2/data/posts.dat", 1);
        });

        Thread thread3 = new Thread(() -> {
            readData("/Users/mariusdragosionita/Documents/workspace/DanielloIonita_2/data/comments.dat", 2);
        });
        thread1.start();
        thread2.start();
        thread3.start();

    }

    public void readData (String filepath, Integer type) {
        String csvFile = filepath;
        String line;
        String cvsSplitBy = "\\|";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                String[] bufferReading = line.split(cvsSplitBy, -1);

                try {
                    // use comma as separator

                    Message m = new Message(type);

                    if (type == 0) {
                        //TODO: remember di reject doble freidnship dude, indian style... jei oh --\_\--   --/_/--
                        m.setTmp(bufferReading[0]);
                        m.setUser_id1(Long.valueOf(bufferReading[1]));
                        m.setUser_id2(Long.valueOf(bufferReading[2]));
                    }
                    else if (type == 1) {
                        m.setTmp(bufferReading[0]);
                        m.setPost_id(Long.valueOf(bufferReading[1]));
                        m.setUser_id1(Long.valueOf(bufferReading[2]));
                        m.setPost(bufferReading[3]);
                        m.setUser_name(bufferReading[4]);
                    }

                    else {
                        m.setTmp(bufferReading[0]);
                        m.setComment_id(Long.valueOf(bufferReading[1]));
                        m.setUser_id1(Long.valueOf(bufferReading[2]));
                        m.setComment(bufferReading[3]);
                        m.setUser_name(bufferReading[4]);
                        if (bufferReading[5].equals("") || bufferReading[5].equals(null) || bufferReading[5].equals(" ")) {
                            m.setComment_replied(null);
                            m.setPost_commented(Long.valueOf(bufferReading[6]));
                        }
                        else {
                            m.setComment_replied(Long.valueOf(bufferReading[5]));
                            m.setPost_commented(null);
                        }
                    }

                    i++;
                    this.sendMessage("localhost", m, "friendshipTopic");
                    if (i>2000) {
                        System.out.println("\n\n\n" + (i*j) + "\n\n\n");
                        i=0;
                        j++;
                    }
                    //System.out.println("send");
                }
                catch (Exception e){
                    checkErrors(bufferReading, type);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void checkErrors (String[] buffer, Integer type) {

        for (String s:buffer){
            if (!consistencyCheck(s))
                System.out.println("Riga non consistente");
        }

        if (type == 0) {
            if (buffer.length != 3)
                printBuffer("Errore numero elementi in friendship", buffer);

            try {
                new SimpleDateFormat(dateFormat).parse(String.valueOf(buffer[0]));
            } catch (ParseException e) {
                printBuffer("Il primo non è un timestamp", buffer);
            }

            try {
                Integer.valueOf(buffer[1]);
                Integer.valueOf(buffer[2]);
            } catch (Exception e) {
                printBuffer("non essere integro 0" , buffer);
            }
        }

        else if (type == 1) {

            if (buffer.length != 5)
                printBuffer("Errore numero elementi in posts" , buffer);

            try {
                new SimpleDateFormat(dateFormat).parse(String.valueOf(buffer[0]));
            } catch (ParseException e) {
                printBuffer("Il primo non è un timestamp" , buffer);
            }

            try {
                Integer.valueOf(buffer[1]);
                Integer.valueOf(buffer[2]);
            } catch (Exception e) {
                printBuffer("non essere integro 1" , buffer);
            }

        }

        else {
            if (buffer.length != 7)
                printBuffer("Errore numero elementi in comments" , buffer);

            try {
                new SimpleDateFormat(dateFormat).parse(String.valueOf(buffer[0]));
            } catch (ParseException e) {
                printBuffer("Il primo non è un timestamp" , buffer);
            }

            try {
                Integer.valueOf(buffer[1]);
                Integer.valueOf(buffer[2]);
                Integer.valueOf(buffer[5]);
                Integer.valueOf(buffer[6]);
            } catch (Exception e) {
                printBuffer("non essere integro 2" , buffer);
            }
        }
    }

    public static void printBuffer (String comment, String[] buffer) {

        System.out.println(comment);
        for (String s:buffer) {
            System.out.print(s + " * ");
        }
        System.out.println();

    }

    public static boolean consistencyCheck(String s) {

        return !s.equals("") && s != null && !s.equals(" ");
    }
}
