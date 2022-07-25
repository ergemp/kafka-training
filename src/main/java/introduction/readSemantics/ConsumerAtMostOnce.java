package introduction.readSemantics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAtMostOnce {

    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.64.7:9092,192.168.64.8:9092,192.168.64.9:9092");
        props.put("group.id", "KafkaAsyncConsumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // mind the configuration
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("testTopic2"));

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records)
            {
                System.out.println(String.format("topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value()));

                //System.out.println(record);
            }

            // mind the configuration
            consumer.commitSync();
            //
        }
    }
}
