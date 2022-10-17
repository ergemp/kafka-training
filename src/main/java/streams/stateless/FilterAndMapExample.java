package streams.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class FilterAndMapExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FilterAndMapExample");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create the streams builder
        final StreamsBuilder builder = new StreamsBuilder();

        // builder.stream("mytopic").to("mytopic-target")
        KStream<String, String> source = builder.stream("mytopic");
        source
                .filter((key, value) -> value.split(" ").length > 2)
                .map((key, value) -> KeyValue.pair(value, value.split(" ").length))
                ;

        // create the topology
        final Topology topology = builder.build();

        // print the topology
        System.out.println(topology.describe());

        // create the streams client
        // with properties and topology
        final KafkaStreams streams = new KafkaStreams(topology, props);

        // streams.start will start the application
        // The execution won't stop until close() is called on this client.
        // streams.start();
    }
}
