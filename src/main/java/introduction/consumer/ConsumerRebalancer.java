package introduction.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ConsumerRebalancer {

    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.64.10:9092,192.168.64.11:9092,192.168.64.12:9092");
        props.put("group.id", "KafkaAsyncConsumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("testTopic"), new consumerRebalanceListener());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records)
            {
                System.out.println(String.format("topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value()));

                //System.out.println(record);
            }
        }
    }

    private static class consumerRebalanceListener implements ConsumerRebalanceListener {
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("ConsumerRebalanceListener: Partition Revoked");
        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for(TopicPartition partition: partitions) {
                //consumer.seekToBeginning(Collections.singleton(partition));
                //consumer.seekToEnd(Collections.singleton(partition));
                //consumer.seek(partition, 9L);
                System.out.println("ConsumerRebalanceListener: Partition Reassigned " + partition.topic() + " - " + partition.partition());
            }
        }
    }
}
