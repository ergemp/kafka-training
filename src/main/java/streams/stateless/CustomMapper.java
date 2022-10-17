package streams.stateless;

import org.apache.commons.cli.*;
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

public class CustomMapper {
    private static String globalJsonKey = "pagetype";
    private static String inputTopic = "input";
    private static String outputTopic = "outEventCounts";
    private static String appID = "streamingAPI_streamCustomMappers";
    private static String bootstrapServers = "localhost:9092";

    public static void main(String[] args) throws Exception
    {

        Options options = new Options();
        options.addOption("globalJsonKey", true, "json key for the event value");
        options.addOption("inputTopic", true, "kafka topic for streaming events");
        options.addOption("outputTopic", true, "kafka topic event counts. This topic will store events counts");
        options.addOption("appID", true, "streaming application ID");
        options.addOption("bootstrapServers", true, "defines kafka broker list");

        try
        {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse( options, args);

            if (cmd.hasOption("globalJsonKey"))
            {
                globalJsonKey = cmd.getOptionValue("globalJsonKey");
            }

            if (cmd.hasOption("leftStream"))
            {
                inputTopic = cmd.getOptionValue("leftStream");
            }

            if (cmd.hasOption("lookupTable"))
            {
                outputTopic = cmd.getOptionValue("lookupTable");
            }

            if (cmd.hasOption("appID"))
            {
                appID = cmd.getOptionValue("appID");
            }

            if (cmd.hasOption("bootstrapServers"))
            {
                bootstrapServers = cmd.getOptionValue("bootstrapServers");
            }
        }
        catch(UnrecognizedOptionException ex)
        {
            System.err.println("Unrecognized option");
            System.err.println(ex.getMessage());
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
        finally
        {
            System.out.println("Options List for exec: ");
            System.out.println("-----------------------");
            System.out.println("globalJsonKey: " + globalJsonKey + " --> Description: " + options.getOption("globalJsonKey").getDescription());
            System.out.println("inputTopic: " + inputTopic + " --> Description: " + options.getOption("inputTopic").getDescription());
            System.out.println("outputTopic: " + outputTopic + " --> Description: " + options.getOption("outputTopic").getDescription());
            System.out.println("appID: " + appID + " --> Description: " + options.getOption("appID").getDescription());
            System.out.println("bootstrapServers: " + bootstrapServers + " --> Description: " + options.getOption("bootstrapServers").getDescription());
            System.out.println("-----------------------");
        }
        final StreamsBuilder builder = new StreamsBuilder();

        final Serde< String > stringSerde = Serdes.String();
        final Serde < Long > longSerde = Serdes.Long();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        System.out.println("Main: properties created");
        System.out.println("------------------------");
        System.out.println(props.toString());
        System.out.println("------------------------");

        System.out.println("Main: creating stream topology");
        KStream<String, String> source = builder.stream(inputTopic);

        source
                .filter(new customFilter())
                .flatMapValues(new customValueMapper())
                .map(new customKVMapper())
                .groupBy(new groupKVMapper())
                .count()
                .mapValues(new stringToLongValueMapper())
                .toStream()
                .to(outputTopic)
        ;

        final Topology topology = builder.build();

        System.out.println("Main: toplogy created");
        System.out.println("---------------------");
        System.out.println(topology.describe());
        System.out.println("---------------------");

        System.out.println("Main: starting stream...");
        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        //Thread.sleep(60000L);
        //streams.close();

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

    public static class customFilter implements Predicate<String, String>
    {
        @Override
        public boolean test(String key, String value)
        {
            if (value.toLowerCase().contains(globalJsonKey))
            {
                System.out.println("customFilter: event contains " + globalJsonKey + " returning true");
                return true;
            }
            else
            {
                System.out.println("customFilter: event doesnt contain " + globalJsonKey + " returning false");
                return false;
            }
        }
    }

    public static class customValueMapper implements ValueMapper<String, Iterable<String>>
    {
        @Override
        public Iterable<String> apply(String value)
        {
            String tmpVal = value;
            String retStr = "";
            tmpVal = tmpVal.replace("{", "").replace("}", "").replace("\"", "");

            String[] arrVal = tmpVal.split(",");

            for (Integer i=0; i<arrVal.length; i++)
            {
                if (arrVal[i].toLowerCase().contains(globalJsonKey))
                {
                    retStr = arrVal[i].split(":")[1];
                }
            }

            List<String> retList = new ArrayList<>();
            retList.add(retStr);

            System.out.println("customValueMapper: input-> " + tmpVal + " output -> " + retList.toString());
            return retList;
        }
    }

    public static class stringToLongValueMapper implements ValueMapper<Long, String>
    {
        @Override
        public String apply(Long value)
        {
            System.out.println("stringToLongValueMapper: returning -> " + Long.toString(value));
            return Long.toString(value);
        }
    }

    public static class customKVMapper implements KeyValueMapper<String, String, KeyValue<String, String>>
    {
        public KeyValue<String, String> apply(String key, String value)
        {
            System.out.println("stringToLongValueMapper: returning -> " + value + " , " + value);
            return new KeyValue<String, String>(value, value);
        }
    }

    public static class groupKVMapper implements KeyValueMapper<String, String, String>
    {
        @Override
        public String apply(String key, String value) {
            System.out.println("groupKVMapper: returning -> value: " + value.toString() + "");
            return value;
        }
    }
}
