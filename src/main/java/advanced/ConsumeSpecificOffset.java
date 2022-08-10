package advanced;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ConsumeSpecificOffset {

    private static KafkaConsumer<String, String> consumer;
    public static long lastOffset;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "ConsumeSpecificOffsetExample-1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("f2d52d8e-26f3-40c1-910a-4c6a001f2589"), new startFromBeginning());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {

                System.out.println(String.format("topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value()));

            }
        }
    }

    private static class startFromBeginning implements ConsumerRebalanceListener {
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for(TopicPartition partition: partitions) {
                //consumer.seekToBeginning(Collections.singleton(partition));
                //consumer.seekToEnd(Collections.singleton(partition));

                consumer.seek(partition, 9L);

                //lastOffset = consumer.position(partition);
                //consumer.seek(partition, lastOffset-10);
            }
        }
    }
}
