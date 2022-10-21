package streams.join;

public class StreamToTable {
    public static void main(String[] args) {
        // KStream-KTable joins are always non-windowed joins.
        // They allow you to perform table lookups against a KTable (changelog stream)
        // upon receiving a new record from the KStream (record stream).
        //
        // An example use case would be to enrich a stream of user activities (KStream)
        // with the latest user profile information (KTable).

        /* inner join */

        /*
        KStream<String, Long> left = ...;
        KTable<String, Double> right = ...;

        // Java 8+ example, using lambda expressions
        KStream<String, String> joined = left.join(right,
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, // ValueJoiner
                Joined.keySerde(Serdes.String()) // key
                        .withValueSerde(Serdes.Long()) // left value
        );

        // Java 7 example
        KStream<String, String> joined = left.join(right,
                new ValueJoiner<Long, Double, String>() {
                    @Override
                    public String apply(Long leftValue, Double rightValue) {
                        return "left=" + leftValue + ", right=" + rightValue;
                    }
                },
                Joined.keySerde(Serdes.String()) // key
                        .withValueSerde(Serdes.Long()) // left value
        );
        */

        /* left join */

        /*
        KStream<String, Long> left = ...;
        KTable<String, Double> right = ...;

        // Java 8+ example, using lambda expressions
        KStream<String, String> joined = left.leftJoin(right,
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, // ValueJoiner
                Joined.keySerde(Serdes.String()) // key
                        .withValueSerde(Serdes.Long()) // left value
        );

        // Java 7 example
        KStream<String, String> joined = left.leftJoin(right,
                new ValueJoiner<Long, Double, String>() {
                    @Override
                    public String apply(Long leftValue, Double rightValue) {
                        return "left=" + leftValue + ", right=" + rightValue;
                    }
                },
                Joined.keySerde(Serdes.String()) // key
                        .withValueSerde(Serdes.Long()) // left value
        );
        */

    }
}
