package controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import entities.Message;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaConsumer {

    Monitor m;

    public void setAttributes(Monitor m) {
        this.m = m;
    }


    private Consumer<Long, String> consumer;
    private final String BOOTSTRAP_SERVERS =
//            "localhost:9092,localhost:9093,localhost:9094";
            "localhost:9092";


    public void subscribeToTopic(String topic) {
        consumer.subscribe(Collections.singletonList(topic));
    }


    private void createConsumer() {


        final Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG,
//                "KafkaExampleConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);

    }


    public void runConsumer(String topic) {

        createConsumer();
        subscribeToTopic(topic);

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);

//            if (consumerRecords.count()==0) {
//                noRecordsCount++;
//                if (noRecordsCount > giveUp) break;
//                else continue;
//            }
            consumerRecords.forEach(this::DeserializeMessage);

            consumer.commitAsync();
        }
//        consumer.close();
//        System.out.println("DONE");
    }


    private void DeserializeMessage(ConsumerRecord<Long, String> record) {
        ObjectMapper mapper = new ObjectMapper();

        //JSON from String to Object
        try {
            Message message = mapper.readValue(record.value(), Message.class);

//            Printer.getInstance().print("Consumer Record:(" + record.key() +
//                    ", Message: " + message.getID() +
//                    ", Code: " + message.getCode() +
//                    ")", "cyan");
            workWithMessage(message);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Message codes:
     * 10:     get the list of semaphores in the crossroad by the controller
     * 301:    the controller has started the voting phase of the 2pc
     * 302:    the controller has started the commit phase of the 2pc
     * -302:   the controller has started the rollback phase of the 2pc
     *
     * @param message
     */
    private void workWithMessage(Message message) {
        m.printReceivedMessage(message);
    }


}


