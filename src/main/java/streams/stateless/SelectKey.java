package streams.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Properties;

public class SelectKey {
    public static void main(String[] args) {
        /*
        Assigns a new key – possibly of a new key type – to each record. (details)

        Calling selectKey(mapper) is the same as calling map((key, value) -> mapper(key, value), value).

        Marks the stream for data re-partitioning: Applying a grouping
        or a join after selectKey will result in re-partitioning of the records.
        */
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "SelectKey");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create the streams builder
        final StreamsBuilder builder = new StreamsBuilder();

        // builder.stream("mytopic").to("mytopic-target")
        KStream<String, String> source = builder.stream("mytopic");

        source.selectKey((key,value) -> value.split(" ")[0]);

        source.selectKey(new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(String key, String value) {
                return value.split(" ")[0];
            }
        });

        // create the topology
        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();

    }
}
