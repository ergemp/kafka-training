package streams.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Grouped;

import java.util.Arrays;
import java.util.Properties;

public class GroupByKey {
    public static void main(String[] args) {

        /*
        * Groups the records by the existing key. (details)
        *
        * Grouping is a prerequisite for aggregating a stream or a table
        * and ensures that data is properly partitioned (“keyed”) for subsequent operations.
        * */

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "GroupByKey-v2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create the streams builder
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream("mytopic")
                .flatMapValues((value) -> Arrays.asList(value.toString().split(" ")))
                .map((key, value) -> new KeyValue<>(value, 1))
                //.peek((key,value) -> System.out.println(key  + " => " + value))
                .groupByKey
                        (   Grouped.with(
                                Serdes.String(),
                                Serdes.Integer())
                        )
                .count()
                .toStream()
                .peek((key,value) -> System.out.println(key  + " => " + value))
        ;

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();
    }
}
