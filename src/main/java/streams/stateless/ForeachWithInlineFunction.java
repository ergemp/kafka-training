package streams.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.ForeachAction;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class ForeachWithInlineFunction {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ForeachWithInlineFunctionExample");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create the streams builder
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream("mytopic")
                .flatMap((key,value) -> {
                    List<KeyValue<String, String>> result = new LinkedList<>();

                    for (String s: value.toString().split(" ")) {
                        result.add(KeyValue.pair("", s));
                    }
                    return result;
                })
                .foreach(new ForeachAction<String, String>() {
                    @Override
                    public void apply(String key, String value) {
                        System.out.println(key + " => " + value);
                    }
                })
        ;

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();
    }
}
