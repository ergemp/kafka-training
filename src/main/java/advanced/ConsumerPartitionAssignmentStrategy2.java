package advanced;

import introduction.consumer.ConsumerRebalancer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ConsumerPartitionAssignmentStrategy2 {

    private static final Logger log = LoggerFactory.getLogger(ConsumerRebalancer.class.getSimpleName());

    public static void main(String[] args) {
        Properties props = new Properties();
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.211.55.3:9092,10.211.55.4:9092,10.211.55.6:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerPartitionAssignmentStrategy");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Collections.singletonList("mytopic"), new consumerRebalanceListener());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value()));
            }
        }

    }

    private static class consumerRebalanceListener implements ConsumerRebalanceListener {
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            for(TopicPartition partition: partitions) {
                //consumer.seekToBeginning(Collections.singleton(partition));
                //consumer.seekToEnd(Collections.singleton(partition));
                //consumer.seek(partition, 9L);
                System.out.println("ConsumerRebalanceListener: Partition Revoked " + partition.topic() + " - " + partition.partition());
            }
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
