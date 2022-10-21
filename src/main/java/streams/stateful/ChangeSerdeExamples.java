package streams.stateful;

public class ChangeSerdeExamples {
    public static void main(String[] args) {

        //
        // consumed with
        //
        /*
        KStream<String, Long> wordCounts = builder.stream(
                "word-counts-input-topic", // input topic
                Consumed.with(
                        Serdes.String(),    // key serde
                        Serdes.Long()       // value serde
                );
        */


        //
        // materialized
        //
        /*
        GlobalKTable<String, Long> wordCounts = builder.globalTable(
        "word-counts-input-topic",
        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(
          "word-counts-global-store" // table/store name )
          .withKeySerde(Serdes.String()) // key serde
                    .withValueSerde(Serdes.Long()) //value serde
        );
        * */

        /*
        KTable<byte[], Long> aggregatedStream = groupedStream.aggregate(
            () -> 0L, // initializer
            (aggKey, newValue, aggValue) -> aggValue + newValue.length(), // adder
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store") // state store name
                        .withValueSerde(Serdes.Long()); // serde for aggregate value
        * */

        /*
        KTable<String, Integer> aggregated = groupedStream.aggregate(
            () -> 0, // initializer
            (aggKey, newValue, aggValue) -> aggValue + newValue, // adder
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>as("aggregated-stream-store" // state store name )
                        .withKeySerde(Serdes.String()) // key serde
                        .withValueSerde(Serdes.Integer()); // serde for aggregate value
        * */


        //
        // grouped with
        //
        /*
        KGroupedStream<byte[], String> groupedStream = stream.groupByKey(
                Grouped.with(
                        Serdes.ByteArray(), // key
                        Serdes.String())    // value
        );
        */

        /*
        KGroupedStream<String, String> groupedStream = stream.groupBy(
                (key, value) -> value,
                Grouped.with(
                        Serdes.String(),  // key (note: type was modified)
                        Serdes.String())  // value
        );
        */

        /*
        KGroupedTable<String, Integer> groupedTable = userProfiles
            .groupBy((user, region) -> KeyValue.pair(region, user.length()), Serdes.String(), Serdes.Integer());
        * */


        //
        // produced with
        //
        /*
        KStream<String, Long> stream = ...;

        // Write the stream to the output topic, using the configured default key
        // and value serdes.
        stream.to("my-stream-output-topic");

        // Write the stream to the output topic, using explicit key and value serdes,
        // (thus overriding the defaults in the config properties).
        stream.to("my-stream-output-topic", Produced.with(Serdes.String(), Serdes.Long());
        * */

    }
}
