package advanced;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumeSpecificOffsetWithoutRebalanceListener3 {

    private static KafkaConsumer<String, String> consumer;
    public static long lastOffset;
    public static Logger logger = LoggerFactory.getLogger(ConsumeSpecificOffsetWithoutRebalanceListener3.class.getName());

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "ConsumeSpecificOffsetWithoutRebalanceListener");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<String, String>(props);

        // assign and seek are mostly used to replay a data or fetch a specific message
        TopicPartition partitionToReadFrom = new TopicPartition("mytopic",0);
        Long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));
        //have to assign in order to use seek

        //seek
        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        Integer numberOfMessagesToRead = 5;
        Integer numberOfMessagesSoFar = 0;
        Boolean keepOnReading = true;

        //poll for new data
        while (keepOnReading) {
            //consumer.poll(100); // deprecated
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0

            for (ConsumerRecord record:records ) {
                numberOfMessagesSoFar ++;
                logger.info("Key: " + record.key() + ",Value: " + record.value());
                logger.info("Partition: " + record.partition() + ",Offset: " + record.offset());
                if (numberOfMessagesSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false; //to exit the while loop
                    break; //exit the loop
                }
            }

        }
        logger.info("exiting application");
    }
}
