package streams.stateless;

import introduction.producer.AsyncProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class Branch {
    private static final Logger log = LoggerFactory.getLogger("BranchExample");

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "BranchExample");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // create the streams builder
        final StreamsBuilder builder = new StreamsBuilder();

        // builder.stream("mytopic").to("mytopic-target")
        KStream<String, String> stream = builder.stream("mytopic");

        Map<String, KStream<String, String>> branches =
                stream.split(Named.as("Branch-"))
                        .branch((key, value) -> value.startsWith("A"),    /* first predicate  */
                                Branched.as("A"))
                        .branch((key, value) -> value.startsWith("B"),    /* second predicate */
                                Branched.as("B"))
                        .defaultBranch(Branched.as("C"))          /* default branch */
        ;

        // KStream branches.get("Branch-A") contains all records whose keys start with "A"
        // KStream branches.get("Branch-B") contains all records whose keys start with "B"
        // KStream branches.get("Branch-C") contains all other records

        branches.get("Branch-A").foreach((k,v) -> {
            log.info(v);
            System.out.println(v);
        });

        final Topology topology = builder.build();

        // print the topology
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }
}
