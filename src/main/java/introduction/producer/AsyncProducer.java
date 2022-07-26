package introduction.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncProducer {

    private static final Logger log = LoggerFactory.getLogger(AsyncProducer.class.getSimpleName());

    public static void main(String[] args) {

        Properties props = new Properties();
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.211.55.9:9092,10.211.55.10:9092,10.211.55.11:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AsyncProducer");  //client.id
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  //key.serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  //value.serializer

        //props.put(ProducerConfig.ACKS_CONFIG, "0" );
        //props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        //props.put(ProducerConfig.BATCH_SIZE_CONFIG, 10);

        //create the producer
        Producer producer = new KafkaProducer(props);
        //KafkaProducer<String, String> producer2 = new KafkaProducer<String, String>(props);

        //create the record
        ProducerRecord<String, String> record = new ProducerRecord<>("mytopic", "test message from java-1");

        try {
            //async producer
            producer.send(record);

            //sync producer
            //details: SyncProducer Class
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
