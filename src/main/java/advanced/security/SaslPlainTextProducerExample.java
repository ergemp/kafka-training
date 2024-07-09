package advanced.security;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SaslPlainTextProducerExample {

    public static void main(String[] args) {
        Properties props = new Properties();
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093,localhost:9094");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "SaslPlainTextProducerExample");  //client.id
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  //key.serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  //value.serializer

        props.put("security.protocol","SASL_PLAINTEXT");
        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"ergemp\" password=\"123456\";");
        props.put("sasl.mechanism","PLAIN");

        //create the producer
        Producer producer = new KafkaProducer(props);

        //create the record
        ProducerRecord<String, String> record = new ProducerRecord<>("mytopic", "test message from java-1");

        try {
            producer.send(record);
            producer.flush();
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
