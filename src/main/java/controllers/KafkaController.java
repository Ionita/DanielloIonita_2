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

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class KafkaController implements Serializer {
    final Producer<Long, String> producer;

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

    public void readData () throws IOException {
        String csvFile = "/Users/mariusdragosionita/Documents/workspace/DanielloIonita_2/data/friendships.dat";
        String line;
        String cvsSplitBy = "\\|";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                try {
                    // use comma as separator
                    String[] bufferReading = line.split(cvsSplitBy);

                    Message m = new Message(0);
                    //m.setTmp(Timestamp.valueOf(bufferReading[0]));
                    m.setUser_id1(Integer.valueOf(bufferReading[1]));
                    m.setUser_id2(Integer.valueOf(bufferReading[2]));

                    this.sendMessage("localhost", m, "friendshipTopic");

                    //System.out.println("send");
                }
                catch (Exception e){
                    //System.out.println("not sending");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
