package streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

public class AnalyticsStreamJob {
    public static void main(String[] args){
        System.out.println("Main: creating properties with the supplied parameters");
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AnalyticsStreamJob-01");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        System.out.println("Main: properties created");
        System.out.println("------------------------");
        System.out.println(props.toString());
        System.out.println("------------------------");

        System.out.println("Main: creating stream topology");
        final StreamsBuilder builder = new StreamsBuilder();

        Duration windowSizeMs = Duration.ofSeconds(10);
        Duration gracePeriodMs = Duration.ofSeconds(10);
        //TimeWindows.of(windowSizeMs).grace(gracePeriodMs);
        // The above is equivalent to the following code:
        //TimeWindows.of(windowSizeMs).advanceBy(windowSizeMs).grace(gracePeriodMs);

        KStream<String, String> source = builder.stream("perf-test");
        ObjectMapper objectMapper = new ObjectMapper();

        //KStream<String, String> events = source
            source
                .map( (mkey, mvalue) -> {
                    try {
                        JsonNode jsonNode = objectMapper.readTree(mvalue);
                        JsonNode eventNode = jsonNode.get("event");

                        if (eventNode != null) {
                            return new KeyValue<String, String>(eventNode.textValue(), "");
                        }
                        else {
                            return new KeyValue<String, String>("null","");
                        }
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                        return new KeyValue<String, String>("error", "");
                    } catch (IOException e) {
                        e.printStackTrace();
                        return new KeyValue<String, String>("error", "");
                    }
                    //System.out.println(mvalue);
                })
                .groupByKey()
                .windowedBy(TimeWindows.of(windowSizeMs).advanceBy(windowSizeMs).grace(gracePeriodMs))
                .count()
            .toStream()
            .foreach((key,value) -> System.out.println(" key : " + key + " value : " + value ));
            ;

        final Topology topology = builder.build();
        System.out.println("Main: toplogy created");
        System.out.println("---------------------");
        System.out.println(topology.describe());
        System.out.println("---------------------");

        System.out.println("Main: starting stream...");
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }
}
