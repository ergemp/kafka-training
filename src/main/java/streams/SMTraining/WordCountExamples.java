package streams.SMTraining;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class WordCountExamples {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "WordCountExamples");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("firstTopic");

        /*
        source.mapValues(new ValueMapper<String, Object>() {
            @Override
            public Object apply(String s) {
                return null;
            }
        });
        */

         KTable<String, Long> counts = source
                .mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .selectKey((key,value) -> value)
                .groupByKey()
                .count();

         /*
        counts.toStream().map(new KeyValueMapper<String, Long, KeyValue<?, ?>>() {
            @Override
            public KeyValue<?, ?> apply(String s, Long aLong) {
                return new KeyValue<String, String>(s, aLong.toString());
            }
        });
        */

        counts.toStream().map((key, value) -> new KeyValue<>(key.toString(), value.toString())).to("secondTopic");

        Topology topology = builder.build();
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
