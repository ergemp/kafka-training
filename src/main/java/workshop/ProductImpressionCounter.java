package workshop;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import util.JsonParser;

import java.util.Arrays;
import java.util.Properties;

public class ProductImpressionCounter {
    public static void main(String[] args) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ProductImpressionCounter-1052-v6");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        try {
            KStream<String, String> source = builder.stream("events_1052");
            JsonParser parser = new JsonParser();

            source
                    .mapValues(value -> value.toLowerCase())
                    .filter((key, value) -> !value.trim().equalsIgnoreCase("") )
                    .mapValues(value -> "{\"row\":["+ parser.parse(value, "impressions") + "]}")
                    .filter((key, value) -> !value.trim().equalsIgnoreCase("{\"row\":[]}"))
                    .mapValues(value -> parser.parse(value, "id"))
                    .filter((key, value) -> !value.trim().equalsIgnoreCase(""))
                    .flatMapValues(value -> Arrays.asList(value.toLowerCase().split(",")))
                    .groupBy((key, value) -> value)
                    .windowedBy(TimeWindows.of(60000L).advanceBy(60000L))
                    //.count(TimeWindows.of(60000L).until(60000L))
                    .count()
                    //.mapValues(value -> Long.toString((Long)value))
                    //.toStream((wk, v) -> wk.toString() + " - " + v)
                    .toStream((wk, v) -> "{\"productid\":\"" + wk.toString().substring(1, wk.toString().indexOf("@")) + "\",\"beginTs\":\"" + wk.toString().substring(wk.toString().indexOf("@")+1,wk.toString().indexOf("/") ) + "\""  + "," + "\"endTs\":\"" + wk.toString().substring(wk.toString().indexOf("/")+1,wk.toString().length()-1 ) + "\",\"count\":" + v + "}")
                    //.to("ProductImpressionCounter-1052-v2")
                    .foreach((key, value) -> System.out.println(key + " --- " + value));
            //.print();

            final Topology topology = builder.build();
            System.out.println(topology.describe());
            final KafkaStreams streams = new KafkaStreams(topology, props);

            System.out.println("starting kafka stream");
            streams.cleanUp();
            streams.start();

            //Thread.sleep(60000L);
            //streams.close();

            //System.gc();
        }
        catch(Exception ex) {
            ex.printStackTrace();
            System.gc();
        }
    }
}
