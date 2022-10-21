package streams.join;

public class StreamToStream {
    public static void main(String[] args) {

        // KStream-KStream joins are always windowed joins,
        // because otherwise the size of the internal state store used to perform the join
        // – e.g., a sliding window or “buffer” – would grow indefinitely.
        //
        // For stream-stream joins it’s important to highlight that a new input record on one side will produce
        // a join output for each matching record on the other side,
        // and there can be multiple such matching records in a given join window
        // (cf. the row with timestamp 15 in the join semantics table below, for example).

        /* inner join */

        /*
        import java.time.Duration;
        KStream<String, Long> left = ...;
        KStream<String, Double> right = ...;

        // Java 8+ example, using lambda expressions
        KStream<String, String> joined = left.join(right,
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, // ValueJoiner
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                Joined.with(
                        Serdes.String(), // key
                        Serdes.Long(),   // left value
                        Serdes.Double()) // right value
        );

        // Java 7 example
        KStream<String, String> joined = left.join(right,
                new ValueJoiner<Long, Double, String>() {
                    @Override
                    public String apply(Long leftValue, Double rightValue) {
                        return "left=" + leftValue + ", right=" + rightValue;
                    }
                },
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                Joined.with(
                        Serdes.String(), // key
                        Serdes.Long(),   // left value
                        Serdes.Double()) // right value
        );
        */

        /* left join */

        /*
         *  import java.time.Duration;
            KStream<String, Long> left = ...;
            KStream<String, Double> right = ...;

            // Java 8+ example, using lambda expressions
            KStream<String, String> joined = left.leftJoin(right,
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, // ValueJoiner
                    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                            Joined.with(
                                    Serdes.String(),  // key
                                    Serdes.Long(),    // left value
                                    Serdes.Double())  // right value
              );

            // Java 7 example
            KStream<String, String> joined = left.leftJoin(right,
                    new ValueJoiner<Long, Double, String>() {
                        @Override
                        public String apply(Long leftValue, Double rightValue) {
                            return "left=" + leftValue + ", right=" + rightValue;
                        }
                    },
                    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                    Joined.with(
                            Serdes.String(), // key
                            Serdes.Long(),   //left value
                            Serdes.Double()) // right value
            );
            * */
    }
}
