package introduction.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AsyncProducer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.64.10:9092,192.168.64.11:9092,192.168.64.12:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaAsyncProducer");  //client.id
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  //key.serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  //value.serializer

        // props.put(ProducerConfig.ACKS_CONFIG, "0" );
        // props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        // props.put(ProducerConfig.BATCH_SIZE_CONFIG, 10);

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(props);
        //KafkaProducer<String, String> producer2 = new KafkaProducer<String, String>(props);

        ProducerRecord<String, String> record =
                new ProducerRecord<>("testTopic", "5", "test message from java-10");

        try {
            //async producer
            producer.send(record);

            //sync producer
            //producer.send(record).get();

            //flush data
            producer.flush();

            //flush and close producer
            producer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
