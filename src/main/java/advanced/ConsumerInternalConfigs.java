package advanced;

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

public class ConsumerInternalConfigs {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerInternalConfigs.class.getName());

        //create consumer configs
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerInternalConfigs");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //latest, earliest, none

        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000"); //default: 10 seconds
        //if no heartbeats is not send during this period, consumer considered to be dead

        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000"); //default: 3 seconds
        //1/3 of the session.timeout.ms

        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000"); //default: 5 min
        //maximum amount between two polls before considering the consumer dead
        //important especially in bigdata processing for batch operations
        //if so increase this value
        //or make the processing faster

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //subscribe to topic(s)
        consumer.subscribe(Collections.singleton("mytopic"));
        //consumer.subscribe(Arrays.asList("first-topic","second-topic","third-topic"));

        //poll for new data
        while (true) {
            //consumer.poll(100); // deprecated
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); // new in kafka 2.0.0

            for (ConsumerRecord record:records ) {
                logger.info("Key: " + record.key() + ",Value: " + record.value());
                logger.info("Partition: " + record.partition() + ",Offset: " + record.offset());
            }
        }
    }
}
