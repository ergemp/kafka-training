package introduction.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerUntilEndOfTopic {

    private static KafkaConsumer<String, String> consumer;

    private static final Logger log = LoggerFactory.getLogger(ConsumerUntilEndOfTopic.class.getSimpleName());

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "ConsumerUntilEndOfTopic");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("mytopic"));

        final int giveUp = 10;
        int noRecordsCount = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            if (records.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }

            if (records.isEmpty()) {
                System.out.println("no more rows.. exiting");

                consumer.commitAsync();
                consumer.close();
                break;
            }

            for (ConsumerRecord<String, String> record : records) {
                log.info(String.format("topic = %s, partition = %s, offset = %d, key = %s, value = %s ",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value()));
            }
        }
    }
}
