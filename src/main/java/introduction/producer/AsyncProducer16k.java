package introduction.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AsyncProducer16k {

    private static final Logger log = LoggerFactory.getLogger(AsyncProducer16k.class.getSimpleName());

    public static void main(String[] args) {

        Properties props = new Properties();
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.211.55.3:9092,10.211.55.4:9092,10.211.55.6:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AsyncProducer16k");  //client.id
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  //key.serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  //value.serializer

        //props.put(ProducerConfig.ACKS_CONFIG, "0" );
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10000);
        //props.put(ProducerConfig.BATCH_SIZE_CONFIG, 10);

        //create the producer
        Producer producer = new KafkaProducer(props);
        //KafkaProducer<String, String> producer2 = new KafkaProducer<String, String>(props);

        try {
            //async producer

            for (Integer i=0; i<200; i++) {
                //create the record
                ProducerRecord<String, String> record = new ProducerRecord<>("mytopic", "1234567890 qwertyasdfgh ---- " + i);

                producer.send(record);
                Thread.sleep(1000);
            }

            //sync producer
            //details: SyncProducer Class
            //producer.send(record).get();

            //flush data
            //producer.flush();

            //flush and close producer
            //producer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
