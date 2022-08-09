package advanced.OffsetCommitExamples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OffsetCommitWithExceptionHandling {

    private static KafkaConsumer<String, String> consumer;
    private static Logger logger = LoggerFactory.getLogger(OffsetCommitWithExceptionHandling.class.getName());

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "OffsetCommitWithExceptionHandling");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.setProperty("max.poll.records","10"); // fetch 10 records at a time

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //because it is true by default
        //so the consumer commits the offsets AFTER they are being processed
        //mind the configuration

        //create the consumer
        consumer = new KafkaConsumer<String, String>(props);

        //subscribe to a topic
        consumer.subscribe(Collections.singletonList("mytopic"));

        //poll for new data
        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            logger.info("Recieved " + records.count() + " records");

            /*
            for (ConsumerRecord<String, String> record : records) {}
            */

            for (ConsumerRecord record:records ) {

                /*
                System.out.println(String.format("topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                */

                //System.out.println(record);

                logger.info("Key: " + record.key() + ",Value: " + record.value());
                logger.info("Partition: " + record.partition() + ",Offset: " + record.offset());

                try{
                    Thread.sleep(10);
                }
                catch(InterruptedException ex){
                    ex.printStackTrace();
                }
            }

            logger.info("Committing the offsets");
            // mind the configuration
            consumer.commitAsync();
            //
            logger.info("Offsets committed");

            try{
                Thread.sleep(1000);
            }
            catch(InterruptedException ex){
                ex.printStackTrace();
            }
            finally {
                try {
                    // commitSync retries committing as long as there is no error that can’t be recovered.
                    // If this happens, there is not much we can do except log an error.

                    // application is blocked until the broker responds to the commit request
                    consumer.commitSync();
                } finally {
                    consumer.close();
                }
            }
        }

    }
}
