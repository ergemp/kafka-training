package streams.source;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

public class CreateKTable {
    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Long> wordCounts = builder.table(
                "mytopic", /* input topic */
                Consumed.with(
                        // If you do not specify Serdes explicitly,
                        // the default Serdes from the configuration are used.
                        Serdes.String(), /* key serde */
                        Serdes.Long()   /* value serde */
                ));

        KTable<String, Long> wordCounts2 = builder.table(
                "mytopic", /* input topic */
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(
                                "word-counts-global-store" /* table/store name */)
                        .withKeySerde(Serdes.String()) /* key serde */
                        .withValueSerde(Serdes.Long()) /* value serde */
        );

    }
}
