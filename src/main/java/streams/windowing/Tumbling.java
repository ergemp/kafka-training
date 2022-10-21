package streams.windowing;

import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;

public class Tumbling {
    public static void main(String[] args) {
        /* Tumbling time windows are a special case of hopping time windows and, like the latter,
         * are windows based on time intervals. They model fixed-size, non-overlapping, gap-less windows.
         *
         * A tumbling window is defined by a single property: the window’s size.
         * A tumbling window is a hopping window whose window size is equal to its advance interval.
         * Since tumbling windows never overlap, a data record will belong to one and only one window.
         */

        // A tumbling time window with a size of 5 minutes (and, by definition, an implicit
        // advance interval of 5 minutes), and grace period of 1 minute.
        Duration windowSize = Duration.ofMinutes(5);
        Duration gracePeriod = Duration.ofMinutes(1);
        //TimeWindows.ofSizeAndGrace(windowSize, gracePeriod);

        // The above is equivalent to the following code:
        //TimeWindows.ofSizeAndGrace(windowSize, gracePeriod).advanceBy(windowSize);
    }
}
