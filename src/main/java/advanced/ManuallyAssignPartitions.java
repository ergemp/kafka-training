package advanced;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ManuallyAssignPartitions {
    public static void main(String[] args) {

        // When you know exactly which partitions the consumer should read, you don’t subscribe to a topic,
        // instead, you assign yourself a few partitions.
        // A consumer can either subscribe to topics (and be part of a consumer group),
        // or assign itself partitions,
        // but not both at the same time

        // Other than the lack of re-balances and the need to manually find the partitions,
        // everything else is business as usual.
        // Keep in mind that if someone adds new partitions to the topic,
        // the consumer will not be notified.
        // You will need to handle this by checking consumer.partitionsFor() periodically
        // or simply by bouncing the application whenever partitions are added.

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        List<PartitionInfo> partitionInfos = null;
        List<TopicPartition> partitions = new ArrayList<>();

        partitionInfos = consumer.partitionsFor("mytopic");

        if (partitionInfos != null) {
            for (PartitionInfo partition : partitionInfos) {
                partitions.add(new TopicPartition(partition.topic(), partition.partition()));
            }

            consumer.assign(partitions);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
                consumer.commitSync();
            }
        }
    }
}
