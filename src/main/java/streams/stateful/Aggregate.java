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
import java.util.Locale;
import java.util.Properties;

public class Aggregate {
    public static void main(String[] args) {
        /* After records are grouped by key via groupByKey or groupBy – and thus represented as
         * either a KGroupedStream or a KGroupedTable, they can be aggregated via an operation such as reduce.
         * Aggregations are key-based operations, which means that they always operate over records
         * (notably record values) of the same key.
         *
         * You can perform aggregations on windowed or non-windowed data. */

        /*
        * Rolling aggregation. Aggregates the values of (non-windowed) records by the grouped key or cogrouped.
        * Aggregating is a generalization of reduce and allows, for example,
        * the aggregate value to have a different type than the input values.
        * (KGroupedStream details, KGroupedTable details KGroupedTable details)

        * When aggregating a grouped stream, you must provide an initializer (e.g., aggValue = 0)
        * and an “adder” aggregator (e.g., aggValue + curValue).
        * When aggregating a grouped table, you must additionally provide a “subtractor” aggregator
        * (think: aggValue - oldValue).

        * When aggregating a cogrouped stream, the actual aggregators are provided for each input stream
        * in the prior cogroup()calls, and thus you only need to provide an initializer (e.g., aggValue = 0)
        * */

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Aggregate");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create the streams builder
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("mytopic");

        KGroupedStream<String, String> groupedStream = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value);

        // Aggregating a KGroupedStream (note how the value type changes from String to Long)
        KTable<String, Long> aggregatedStream = groupedStream.aggregate(
                () -> 0L, /* initializer */
                (aggKey, newValue, aggValue) -> aggValue + newValue.length(), /* adder */
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store") /* state store name */
                        .withValueSerde(Serdes.Long())); /* serde for aggregate value */

        aggregatedStream.toStream().print(Printed.toSysOut());

        final Topology topology = builder.build();

        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();

        /* Input records with null keys are ignored.
         * When a record key is received for the first time, the initializer is called (and called before the adder).
         * Whenever a record with a non-null value is received, the adder is called.
         * */
    }
}
