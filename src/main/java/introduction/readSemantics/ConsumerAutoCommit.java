package introduction.readSemantics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAutoCommit {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerAutoCommit.class.getName());

        //create consumer configs
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerAutoCommit-01");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //latest, earliest, none

        //by default your consumer is at least once
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        //because it is true by default
        //so the consumer commits the offsets AFTER they are being processed

        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"5000");
        //by default, auto commit is enabled and commits the offsets every 5 seconds in regular for every .poll()

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //subscribe to topic(s)
        consumer.subscribe(Collections.singleton("mytopic"));
        //consumer.subscribe(Arrays.asList("first-topic","second-topic","third-topic"));

        //poll for new data
        while (true) {
            //consumer.poll(100); // deprecated
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0

            for (ConsumerRecord record:records ) {
                logger.info("Key: " + record.key() + ",Value: " + record.value());
                logger.info("Partition: " + record.partition() + ",Offset: " + record.offset());
            }
        }
    }
}
