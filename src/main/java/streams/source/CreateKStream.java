package streams.source;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public class CreateKStream {
    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Long> wordCounts = builder.stream(
                "mytopic", /* input topic */
                Consumed.with(
                        // If you do not specify Serdes explicitly,
                        // the default Serdes from the configuration are used.
                        Serdes.String(), /* key serde */
                        Serdes.Long()   /* value serde */
                ));


    }
}
