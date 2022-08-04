package advanced;

import introduction.producer.AsyncProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GracefulShutdownExample {

    private static KafkaConsumer<String, String> consumer;
    private static final Logger log = LoggerFactory.getLogger(GracefulShutdownExample.class.getSimpleName());

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "GracefulShutdownExample");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //current thread reference
        final Thread mainThread = Thread.currentThread();

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("mytopic"));

        //shutdown hook runs on its own thread
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                //instead of running poll
                //raise a wakeup exception
                consumer.wakeup();

                try {
                    mainThread.join();
                }
                catch(InterruptedException iex){
                    iex.printStackTrace();
                }
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
            }
        }
        catch (WakeupException wex){
            log.info("wakeup exception catched");
        }
        catch (Exception ex){
            log.error("exception catched");
            ex.printStackTrace();
        }
        finally {
            //commit and flush the offsets (graceful close)
            log.info("closing the consumer");
            consumer.close();
        }
    }
}
