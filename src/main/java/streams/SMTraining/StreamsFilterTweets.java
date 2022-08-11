package streams.SMTraining;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.util.Properties;

public class StreamsFilterTweets {

    private static JsonParser parser ;
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static Integer extractFollowers(String tweet)  {
        try {
            return Integer.parseInt(objectMapper.readTree(tweet).get("user").get("followersCount").asText());
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public static void main(String[] args) {
        //configuration
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"StreamsFilterTweets-v1");

        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create the topology
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputTopic = builder.stream("mytopic");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k,v) -> extractFollowers(v) > 1000
        );

        filteredStream.to("targettopic");

        //build the topology
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        //start the streams application
        streams.start();
    }
}
