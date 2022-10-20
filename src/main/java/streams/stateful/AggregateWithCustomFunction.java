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

public class AggregateWithCustomFunction {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AggregateWithCustomFunction");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create the streams builder
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("mytopic");

        KGroupedStream<String, String> groupedStream = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value);

        groupedStream
                .aggregate( new myCustomInitializer(),
                            new myCustomAggregator(),
                            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store")
                                        .withValueSerde(Serdes.Long()))
                .toStream()
                .print(Printed.toSysOut());

        final Topology topology = builder.build();

        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();

    }

    static class myCustomAggregator implements Aggregator<String, String, Long>{
        @Override
        public Long apply(String aggKey, String newValue, Long aggValue) {
            return aggValue + newValue.length();
        }
    }

    static class myCustomInitializer implements Initializer<Long>{
        @Override
        public Long apply() {
            return 0L;
        }
    }
}
