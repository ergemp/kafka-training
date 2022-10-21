package streams.windowing;

import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;

public class Hopping {
    public static void main(String[] args) {
        /* Hopping time windows are windows based on time intervals.
         * They model fixed-sized, (possibly) overlapping windows.
         *
         * A hopping window is defined by two properties: the window’s size and its advance interval (aka “hop”).
         *
         * The advance interval specifies by how much a window moves forward relative to the previous one.
         * For example, you can configure a hopping window with a size 5 minutes and an advance interval of 1 minute.
         * Since hopping windows can overlap – and in general they do – a data record may belong to more than one such windows.
         *
         * Hopping windows vs. sliding windows: Hopping windows are sometimes called “sliding windows”
         * in other stream processing tools. Kafka Streams follows the terminology in academic literature,
         * where the semantics of sliding windows are different to those of hopping windows.
         *
         * */

        // A hopping time window with a size of 5 minutes and an advance interval of 1 minute.
        // The window's name -- the string parameter -- is used to e.g. name the backing state store.
        Duration windowSize = Duration.ofMinutes(5);
        Duration advance = Duration.ofMinutes(1);
        TimeWindows.of(windowSize).advanceBy(advance);

    }
}
