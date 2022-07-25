package introduction.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncProducer {

    private static final Logger log = LoggerFactory.getLogger(AsyncProducer.class.getSimpleName());

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "SyncProducer");  //client.id
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  //key.serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  //value.serializer

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(props);

        ProducerRecord<String, String> record =
                new ProducerRecord<>("mytopic","test message from java-10");

        try {
            //sync producer
            RecordMetadata metadata = (RecordMetadata) producer.send(record).get();

            log.info("topic: " + metadata.topic() + " - " +
                    "partition: " + metadata.partition() + " - " +
                    "offset: " + metadata.offset() +  " - " +
                    "timestamp: " + metadata.timestamp() +  " - ");

            //flush data
            producer.flush();

            //flush and close producer
            producer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
