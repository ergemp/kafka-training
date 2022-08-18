package introduction.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MessageMetadata {

    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        Properties props = new Properties();
        //props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.211.55.3:9092,10.211.55.4:9092,10.211.55.6:9092");
        props.put("group.id", "MessageMetadata");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("mytopic"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received message: (" + record.key() + ", " +
                        record.value() + ")  at offset:  " +
                        record.offset() + "  at partition:  " +
                        record.partition() + " at timestamp: " +
                        record.timestamp() + " and the headers are: " +
                        record.headers().toString()
                );
            }
        }
    }
}
