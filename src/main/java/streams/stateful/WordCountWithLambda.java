package streams.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Arrays;
import java.util.Properties;

public class WordCountWithLambda {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "WordCountWithLambda");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create the streams builder
        final StreamsBuilder builder = new StreamsBuilder();
        // builder.stream("mytopic").to("mytopic-target")
        KStream<String, String> source = builder.stream("mytopic");

        KStream<String, Long> wordCounts = source
                // Split each text line, by whitespace, into words.  The text lines are the record
                // values, i.e. you can ignore whatever data is in the record keys and thus invoke
                // `flatMapValues` instead of the more generic `flatMap`.
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                // Group the stream by word to ensure the key of the record is the word.
                .groupBy((key, word) -> word)
                // Count the occurrences of each word (record key).
                //
                // This will change the stream type from `KGroupedStream<String, String>` to
                // `KTable<String, Long>` (word -> count).
                .count()
                // Convert the `KTable<String, Long>` into a `KStream<String, Long>`.
                .toStream()
                ;

        wordCounts.print(Printed.toSysOut());

        final Topology topology = builder.build();

        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();

    }
}
