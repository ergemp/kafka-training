package advanced.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class HighThroughputProducer {
    public static void main(String[] args) {
        //after kafka >= 0.11
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Safe parameters
        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,"120000"); //default 2 minutes
        props.setProperty(ProducerConfig.ACKS_CONFIG, "ALL"); //default 1
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "2000000000"); // default Integer.MAXINT
        props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG , "100"); //default 100ms
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //default 5, set to 1 if ordering is a must

        //implies ack=ALL, retries=Integer.MAX_VALUE, max.inflight.requests.per.connection=1 or 5
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        //Performance Parameters
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");  //number of miliseconds a producer is willing to wait before sending a batch out, default 0
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32768"); //if the batch size is full, then send out the batch without waiting linger.ms, default 16k, allocated per partition
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy"); //default none

        //buffer.memory
        //

        //buffer messages in memory if broker is slow
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(32*1024*1024)); // default 32 MB

        //if buffer full wait this much of time while .send()
        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000"); //default 60 seconds

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //create record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("mytopic", null,"my message");

        try {
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        //handle the error
                        e.printStackTrace();
                    }
                }
            });
        }
        catch (NotEnoughReplicasException nere){
            System.out.println(nere.getMessage());
            System.out.println("NotEnoughReplicasException");
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        finally {
            //flush data
            producer.flush();

            //flush and close producer
            producer.close();
        }
    }
}
