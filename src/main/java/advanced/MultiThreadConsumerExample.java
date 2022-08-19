package advanced;

import introduction.consumer.ConsumerRebalancer;
import org.apache.kafka.clients.consumer.*;

import java.util.*;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiThreadConsumerExample {

    private static final Logger log = LoggerFactory.getLogger(ConsumerRebalancer.class.getSimpleName());

    private static final int numberOfConsumers = 2;
    private static List<ConsumerThread> consumers;

    public static void main(String[] args) {
        consumers = new ArrayList<>();
        for (int i = 0; i < numberOfConsumers; i++) {
            ConsumerThread ncThread = new ConsumerThread();
            consumers.add(ncThread);
        }

        for (ConsumerThread ncThread : consumers) {
            Thread t = new Thread(ncThread);
            t.start();
        }
    }
}

class ConsumerThread implements Runnable {

    private final Consumer<String, String> localConsumer;

    public ConsumerThread() {
        localConsumer = ConsumerCreator.createConsumer();
        localConsumer.subscribe(Collections.singletonList("mytopic"), new consumerRebalanceListener());
    }

    @Override
    public void run() {
        while (true) {

            ConsumerRecords<String, String> records = localConsumer.poll(1000);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Receive message: " + record.value() + ", Partition: "
                        + record.partition() + ", Offset: " + record.offset() + ", by ThreadID: "
                        + Thread.currentThread().getId() + ", ThreadName: " + Thread.currentThread().getName() );

            }
        }
    }
}

class consumerRebalanceListener implements ConsumerRebalanceListener {
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


class ConsumerCreator {
    public static Consumer<String, String> createConsumer() {
        Properties props = new Properties();
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.211.55.3:9092,10.211.55.4:9092,10.211.55.6:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "MultiThreadConsumerExample-0");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);  //max records to poll for each poll method

        return new KafkaConsumer<String, String>(props);
    }
}



