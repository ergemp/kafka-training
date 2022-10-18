package streams.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Arrays;
import java.util.Properties;

public class GroupByWithInlineFunction {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "GroupByWithInlineFunction-v2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create the streams builder
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream("mytopic")
                .flatMapValues((value) -> Arrays.asList(value.toString().split(" ")))
                .map((key, value) -> new KeyValue<>(value, 1))
                //.peek((key,value) -> System.out.println(key  + " => " + value))
                .groupBy
                        (new KeyValueMapper<String, Integer, String>() {
                                @Override
                                public String apply(String key, Integer value) {
                                    return key;
                                }
                            }, Grouped.with(
                                Serdes.String(),
                                Serdes.Integer())
                        )
                .count()
                .toStream()
                .peek((key, value) -> System.out.println(key  + " => " + value))
        ;

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();
    }
}
