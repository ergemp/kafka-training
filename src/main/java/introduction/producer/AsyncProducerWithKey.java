package introduction.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AsyncProducerWithKey {
    private static final Logger log = LoggerFactory.getLogger(AsyncProducer.class.getSimpleName());

    public static void main(String[] args) {

        Properties props = new Properties();
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.211.55.9:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AsyncProducerWithKey");  //client.id
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  //key.serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  //value.serializer

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(props);

        ProducerRecord<String, String> record =
                new ProducerRecord<>("mytopic", "2", "test message from java-k1");

        try {
            //async producer
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception ex) {
                    if (ex == null){
                        // the record was successfully sent
                        log.info("Topic: " + metadata.topic() + " - " +
                                "Partition: " + metadata.partition() + " - " +
                                "Offset: " + metadata.offset() + " - " +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing: ", ex.getMessage());
                    }
                }
            });

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
