package streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.Properties;

public class StreamToTopic {
    public static void main(String[] args) throws Exception
    {
        final StreamsBuilder builder = new StreamsBuilder();

        final Serde< String > stringSerde = Serdes.String();
        final Serde < Long > longSerde = Serdes.Long();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamingAPI-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.110:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStream<String, String> source = builder.stream("test");

        source
                .filter((key, value) -> value.toLowerCase().contains("pagetype") )
                .mapValues(value -> value.replace("{","").replace("}","").replace("\"","").split(":")[(Arrays.asList(value.replace("{","").replace("}","").replace("\"","").split(":")).indexOf("pagetype")+1)])
                .map((mkey, mvalue) -> KeyValue.pair(mvalue, mvalue))
                .groupBy((key, value) -> value)
                .count()
                .mapValues(value -> Long.toString(value)).toStream()
                .to("outEventCounts")
        ;

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        // Thread.sleep(60000L);
        // streams.close();

        /*
        //test data set
        {"ts":1525207511,"a":53,"url":"commercedemo.xenn.io/ajax-full-zip-sweatshirt.html","rf":"","ecommerce":{"currencyCode":"USD"},"pageType":"catalog_product_view","list":"detail","product":{"id":"243","sku":"MH12","name":"Ajax Full-Zip Sweatshirt ","price":69,"attribute_set_id":"9","path":"Men > Tops > Hoodies & Sweatshirts > Ajax Full-Zip Sweatshirt "},"gtm.start":1525207509279,"event":"gtm.dom","gtm.uniqueEventId":1,"nm":"PV","pid":"82478-59608-86276","sid":"21145-32562-59600"}

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
        catalog_product_view    5
        catalog_product_view    6
        catalog_product_view    7
        */
    }
}
