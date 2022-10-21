package streams.windowing;

public class SuppressWindows {
    public static void main(String[] args) {
        /*
        KGroupedStream<UserId, Event> grouped = ...;

        grouped
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(10)))
            .count()
            .suppress(Suppressed.untilWindowCloses(unbounded()))
            .filter((windowedUserId, count) -> count < 3)
            .toStream()
            .foreach((windowedUserId, count) -> sendAlert(windowedUserId.window(), windowedUserId.key(), count));

            ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(10))
                The specified grace period of 10 minutes (i.e., the Duration.ofMinutes(10) argument)
                allows us to bound the lateness of events the window will accept.
                For example, the 09:00 to 10:00 window will accept out-of-order records until 10:10,
                at which point, the window is closed.

            .suppress(Suppressed.untilWindowCloses(...))
                This configures the suppression operator to emit nothing for a window until it closes,
                and then emit the final result. For example, if user U gets 10 events between 09:00 and 10:10,
                the filter downstream of the suppression will get no events for the windowed key U@09:00-10:00 until 10:10,
                and then it will get exactly one with the value 10. This is the final result of the windowed count.

            unbounded()
                This configures the buffer used for storing events until their windows close.
                Production code is able to put a cap on the amount of memory to use for the buffer,
                but this simple example creates a buffer with no upper bound.
        * */

        // rate limiting the supress
        /*
        KGroupedTable<String, String> groupedTable = ...;
        groupedTable
                .count()
                .suppress(untilTimeLimit(ofMinutes(5), maxBytes(1_000_000L).emitEarlyWhenFull()))
                .toStream()
                .foreach((key, count) -> updateCountsDatabase(key, count));

         This configuration ensures that updateCountsDatabase gets events for each key no more than once every 5 minutes.
         Note that the latest state for each key has to be buffered in memory for that 5-minute period.
         You have the option to control the maximum amount of memory to use for this buffer (in this case, 1MB).
         There is also an option to impose a limit in terms of number of records (or to leave both limits unspecified).
         */

    }
}
