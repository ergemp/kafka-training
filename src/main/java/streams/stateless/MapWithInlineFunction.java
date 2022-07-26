package streams.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Properties;

public class MapWithInlineFunction {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "MapWithInlineFunction");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create the streams builder
        final StreamsBuilder builder = new StreamsBuilder();

        // builder.stream("mytopic").to("mytopic-target")
        KStream<String, String> source = builder.stream("mytopic");


        source
                .map(new KeyValueMapper<String, String, KeyValue<String, Integer>>() {
                    @Override
                    public KeyValue<String, Integer> apply(String key, String value) {
                        //return KeyValue.pair(value.toLowerCase(), value.length());
                        return new KeyValue<String, Integer>(value.toLowerCase(), value.length());
                    }
                })
                .filter((key, value) -> value > 5)
                .foreach((key, value) -> System.out.println(key + " => " + value))
        ;


        // create the topology
        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();
    }
}
