package streams.customSerdeExample.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class GetStreamsProperties {
    public static Properties get(String gApplicationId){
        //generate a properties object
        Properties props = new Properties();

        //set the properties
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, gApplicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "ip-172-31-93-96.ec2.internal:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000);

        //return the properties
        return props;
    }
}
