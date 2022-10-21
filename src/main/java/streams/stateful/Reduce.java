package streams.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class Reduce {
    public static void main(String[] args) {

        /*
         * Rolling aggregation. Combines the values of (non-windowed) records by the grouped key.
         * The current record value is combined with the last reduced value, and a new reduced value is returned.
         *
         * The result value type cannot be changed, unlike aggregate
         * */

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Reduce-v3");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create the streams builder
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("mytopic");

        KGroupedStream<String, Integer> groupedStream = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .map((key, value) -> new KeyValue<>(value, value.length()))
                //.peek((key,value) -> System.out.println("key: " + key + "=>" + "value: " + value + " Value type:" + value.getClass() ))
                .groupByKey( Grouped.with(
                                Serdes.String(),
                                Serdes.Integer()) )
                ;

        KTable<String, Integer> reducedStream = groupedStream.reduce(
                (aggValue, newValue) -> aggValue + newValue, //adder
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("word-totals")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Integer()));

        reducedStream.toStream().print(Printed.toSysOut());

        final Topology topology = builder.build();

        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();
    }
}
