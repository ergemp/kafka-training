package streams.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class GroupByCountWithInlineFunction {
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

        KStream<String, String> words = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String value) {
                return Arrays.asList(value.split("\\W+"));
            }
        });

        KTable<String, Long> counts = words.groupBy(new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(String key, String value) {
                return value;
            }
        })
        // Materialize the result into a KeyValueStore named "counts-store".
        // The Materialized store is always of type <Bytes, byte[]> as this is the format of the innermost store.
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>> as("counts-store"));

        // Note that the count operator has a Materialized parameter that specifies that
        // the running count should be stored in a state store named counts-store.
        // This counts-store store can be queried in real-time,


        // We can also write the counts KTable's changelog stream back into another Kafka topic,
        // say streams-wordcount-output.
        // Because the result is a changelog stream,
        // the output topic streams-wordcount-output should be configured with log compaction enabled.
        //
        // Note that this time the value type is no longer String but Long,
        // so the default serialization classes are not viable for writing it to Kafka anymore.
        //
        // We need to provide overridden serialization methods for Long types, otherwise a runtime exception will be thrown:
        counts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        // Note that in order to read the changelog stream from topic streams-wordcount-output,
        // one needs to set the value deserialization as org.apache.kafka.common.serialization.LongDeserializer

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
