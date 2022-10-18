package streams.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;

public class FlatMapValuesWithCustomFunction {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FlatMapValuesExample");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create the streams builder
        final StreamsBuilder builder = new StreamsBuilder();
        // builder.stream("mytopic").to("mytopic-target")
        KStream<String, String> source = builder.stream("mytopic");

        KStream<String, String> words = source.flatMapValues(new myCustomMapper());

        words.to("mytopic-target");

        // create the topology
        final Topology topology = builder.build();

        // print the topology
        System.out.println(topology.describe());

        // create the streams client
        // with properties and topology
        final KafkaStreams streams = new KafkaStreams(topology, props);

        // streams.start will start the application
        // The execution won't stop until close() is called on this client.
         streams.start();
    }

    private static class myCustomMapper implements ValueMapper<String, Iterable<String>> {
        @Override
        public Iterable<String> apply(String s) {
            return Arrays.asList(s.split("\\W+"));        }
    }
}
