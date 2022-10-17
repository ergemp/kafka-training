package streams.stateless;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CustomFilter {
    public static void main(String[] args) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();

        final Serde< String > stringSerde = Serdes.String();
        final Serde < Long > longSerde = Serdes.Long();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "CustomFilter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStream<String, String> source = builder.stream("mytopic");

        KStream<String, String> events = source
                .filter(new customFilter())
                .flatMapValues(new customValueMapper())
                .map(new customKVMapper())
                ;

        events.to("outEvents");

        /*
        KTable<String, Long> counts = events
                .groupBy(new streamingAPI.groupKVMapper())
                .count();
        */

        /*
        counts.toStream().flatMapValues(new streamingAPI.customKeytoValueMapper(), Serialized.with(
                            Serdes.String(),
                            Serdes.String()))
                         .to("outEventCounts");
        */

        /* WORKING EXAMPLE
        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> source = builder.stream("wordcount-input");


        final Pattern pattern = Pattern.compile("\\W+");
        KStream counts  = source.flatMapValues(value-> Arrays.asList(pattern.split(value.toLowerCase())))
                .map((key, value) -> new KeyValue<Object, Object>(value, value))
                .filter((key, value) -> (!value.equals("the")))
                .groupByKey()
                .count("CountStore").mapValues(value->Long.toString(value)).toStream();
        counts.to("wordcount-output");

        KafkaStreams streams = new KafkaStreams(builder, props);
        */

        KStream<String, String> counts = events
                .groupBy(new groupKVMapper())

                .count().mapValues(value-> Long.toString(value)).toStream();
        //.count();

        counts.to("outEventCounts");

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.cleanUp();
        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        //Thread.sleep(60000L);
        //streams.close();

        /*
        bin/kafka-console-consumer.sh --bootstrap-server 192.168.56.110:9092 --topic outEventCounts --property print.key=true --from-beginning
        catalog_product_view
        catalog_product_view
        catalog_product_view    1
        catalog_product_view    2
        catalog_product_view    24
        catalog_product_view    26
        catalog_category_view   1
        catalog_product_view    27
        catalog_category_view   3
        */
    }

    public static class customFilter implements Predicate<String, String> {
        @Override
        public boolean test(String key, String value) {
            if (value.toLowerCase().contains("pagetype")) {
                return true;
            }
            else {
                return false;
            }
        }
    }

    public static class customValueMapper implements ValueMapper<String, Iterable<String>> {
        @Override
        public Iterable<String> apply(String value) {
            String tmpVal = value;
            String retStr = "";
            tmpVal = tmpVal.replace("{", "").replace("}", "").replace("\"", "");

            String[] arrVal = tmpVal.split(",");

            for (Integer i=0; i<arrVal.length; i++) {
                if (arrVal[i].toLowerCase().contains("pagetype")) {
                    retStr = arrVal[i].split(":")[1];
                }
            }

            List<String> retList = new ArrayList<>();
            retList.add(retStr);

            return retList;
        }
    }

    public static class customKVMapper implements KeyValueMapper<String, String, KeyValue<String, String>> {
        public KeyValue<String, String> apply(String key, String value) {
            return new KeyValue<String, String>(value, value);
        }
    }

    public static class customKeytoValueMapper implements KeyValueMapper<String, Long, KeyValue<String, String>> {
        public KeyValue<String, String> apply(String key, Long value) {
            return new KeyValue<String, String>("", key.toString() + " - " + value.toString());
        }
    }

    public static class groupKVMapper implements KeyValueMapper<String, String, String> {
        @Override
        public String apply(String key, String value) {
            //System.out.println(key.toString() + " - " + value.toString());
            return value;
        }
    }

    public static class sinkValueMapper implements ValueMapper<String, Long> {
        @Override
        public Long apply(String value) {
            //System.out.println(key.toString() + " - " + value.toString());
            return Long.getLong(value);
        }
    }
}
