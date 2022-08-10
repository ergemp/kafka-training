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

public class ConsumerPollBehaviour {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerPollBehaviour.class.getName());

        //create consumer configs
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerPollBehaviour");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  //latest, earliest, none

        props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,"1");
        //default: 1
        //Controls how much data you want to pull at least on each request
        //improve throughput, decrease request number
        //at the cost of latency

        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"500");
        //default: 500
        //how many records to pull per request
        //increase if your messages are too small and have a lot of available memory

        props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,"1024000");
        //default: 1MB
        //maximum ata returned by the broker per partition
        //if you read from 100 partitions you need a lot of ram

        props.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,"50000000");
        //default: 50MB
        //maximum data returned for each fetch request (covers multiple partitions)
        //consumer performs multiple fetch requests in parallel

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
