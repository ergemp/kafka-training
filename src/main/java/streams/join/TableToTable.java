package streams.join;

public class TableToTable {
    public static void main(String[] args) {
        // KTable-KTable equi-joins are always non-windowed joins.
        // They are designed to be consistent with their counterparts in relational databases.
        // The changelog streams of both KTables are materialized into local state stores
        // to represent the latest snapshot of their table duals.
        //
        // The join result is a new KTable that represents the changelog stream of the join operation.

        /* inner join */
        /*
        KTable<String, Long> left = ...;
        KTable<String, Double> right = ...;

        // Java 8+ example, using lambda expressions
        KTable<String, String> joined = left.join(right,
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue //ValueJoiner
        );

        // Java 7 example
        KTable<String, String> joined = left.join(right,
                new ValueJoiner<Long, Double, String>() {
                    @Override
                    public String apply(Long leftValue, Double rightValue) {
                        return "left=" + leftValue + ", right=" + rightValue;
                    }
                });

         */


        /* left join */
        /*
        KTable<String, Long> left = ...;
        KTable<String, Double> right = ...;

        // Java 8+ example, using lambda expressions
        KTable<String, String> joined = left.leftJoin(right,
                (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue //ValueJoiner
        );

        // Java 7 example
        KTable<String, String> joined = left.leftJoin(right,
                new ValueJoiner<Long, Double, String>() {
                    @Override
                    public String apply(Long leftValue, Double rightValue) {
                        return "left=" + leftValue + ", right=" + rightValue;
                    }
                });
         */


    }
}
