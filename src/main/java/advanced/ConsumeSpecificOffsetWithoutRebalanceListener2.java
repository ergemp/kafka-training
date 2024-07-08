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

public class ConsumeSpecificOffsetWithoutRebalanceListener2 {
    private static KafkaConsumer<String, String> consumer;
    public static long lastOffset;
    public static Logger logger = LoggerFactory.getLogger(ConsumeSpecificOffsetWithoutRebalanceListener.class.getName());

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.211.55.9:9092");
        props.put("group.id", "ConsumeSpecificOffsetWithoutRebalanceListener");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<String, String>(props);

        // assign and seek are mostly used to replay a data or fetch a specific message
        TopicPartition partitionToReadFrom0 = new TopicPartition("mytopic",0);
        TopicPartition partitionToReadFrom1 = new TopicPartition("mytopic",1);
        TopicPartition partitionToReadFrom2 = new TopicPartition("mytopic",2);

        Long offsetToReadFrom = 1L;
        consumer.assign(Arrays.asList(partitionToReadFrom0,partitionToReadFrom1,partitionToReadFrom2));
        //have to assign in order to use seek

        //seek
        consumer.seek(partitionToReadFrom0,offsetToReadFrom);
        consumer.seek(partitionToReadFrom1,offsetToReadFrom);
        consumer.seek(partitionToReadFrom2,offsetToReadFrom);

        Integer numberOfMessagesToRead0 = 2;
        Integer numberOfMessagesToRead1 = 2;
        Integer numberOfMessagesToRead2 = 2;

        Integer numberOfMessagesSoFar0 = 0;
        Integer numberOfMessagesSoFar1 = 0;
        Integer numberOfMessagesSoFar2 = 0;

        Boolean keepOnReading = true;

        //poll for new data
        while (keepOnReading) {
            //consumer.poll(100); // deprecated
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0

            for (ConsumerRecord record:records ) {


                if (record.partition() == 0  && numberOfMessagesSoFar0 < numberOfMessagesToRead0) {
                    numberOfMessagesSoFar0++;
                    logger.info("Key: " + record.key() + ",Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ",Offset: " + record.offset());

                }

                if (record.partition() == 1 && numberOfMessagesSoFar1 < numberOfMessagesToRead1) {
                    numberOfMessagesSoFar1++;
                    logger.info("Key: " + record.key() + ",Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ",Offset: " + record.offset());

                }

                if (record.partition() == 2 && numberOfMessagesSoFar2 < numberOfMessagesToRead2) {
                    numberOfMessagesSoFar2++;
                    logger.info("Key: " + record.key() + ",Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ",Offset: " + record.offset());

                }
            }

        }
        logger.info("exiting application");
    }
}
