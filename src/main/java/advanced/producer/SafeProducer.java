package advanced.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SafeProducer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,"120000"); //default 2 minutes
        props.setProperty(ProducerConfig.ACKS_CONFIG, "ALL"); //default 1
        props.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE)); // default Integer.MAX_VLAUE
        props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG , "100"); //default 100ms

        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //default 5 as of kafka 2.4, set to 1 if ordering is a must or kafka < 1.1

        //important !!.
        //set min.insync.replicas = 2 in broker or topic level

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
        finally
        {
            //flush data
            producer.flush();

            //flush and close producer
            producer.close();
        }

    }
}
