package advanced;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class CommitOnPartitionRevoked {

    private static KafkaConsumer<String, String> consumer;

    private static class RebalanceHandler implements ConsumerRebalanceListener {
        private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("Lost partitions in rebalance. Committing current offsets:" + currentOffsets);
            consumer.commitSync(currentOffsets);
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "CommitOnPartitionRevoked");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("f2d52d8e-26f3-40c1-910a-4c6a001f2589"), new RebalanceHandler());

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
                consumer.commitAsync();
            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }
}
