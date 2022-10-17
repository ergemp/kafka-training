package streams.windowing;

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

public class TumblingWindow {
    private static String globalJsonKey = "pagetype";
    private static String inStream = "test";
    private static String outStream = "outEventCounts-10Sec";
    private static String appID = "streamingAPI-tumblingwindow10sec";
    private static String bootstrapServers = "localhost:9092";

    public static void main(String[] args) throws Exception
    {

        Options options = new Options();
        options.addOption("globalJsonKey", true, "json key for the event value");
        options.addOption("inStream", true, "kafka topic for streaming events");
        options.addOption("outStream", true, "kafka topic for cumulative events");
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
                inStream = cmd.getOptionValue("inStream");
            }

            if (cmd.hasOption("lookupTable"))
            {
                outStream = cmd.getOptionValue("outStream");
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
            System.out.println("leftStream: " + inStream + " --> Description: " + options.getOption("inStream").getDescription());
            System.out.println("lookupTable: " + outStream + " --> Description: " + options.getOption("outStream").getDescription());
            System.out.println("appID: " + appID + " --> Description: " + options.getOption("appID").getDescription());
            System.out.println("bootstrapServers: " + bootstrapServers + " --> Description: " + options.getOption("bootstrapServers").getDescription());
            System.out.println("-----------------------");
        }


        //System.exit(0);


        final StreamsBuilder builder = new StreamsBuilder();

        final Serde< String > stringSerde = Serdes.String();
        final Serde < Long > longSerde = Serdes.Long();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStream<String, String> source = builder.stream(inStream);

        KStream<String, String> events = source
                .filter(new customFilter())
                .flatMapValues(new customValueMapper())
                .map(new customKVMapper())
                ;

        events.to("outEvents");

/*
        events
                .groupBy(new groupKVMapper())
                .count(TimeWindows.of(10000L).until(10000L))
                .mapValues(value-> Long.toString(value))
                .toStream((wk, v) -> wk.key())
                .to(outStream);
*/
        /*
        events
            .groupBy((key, value) -> value)
            .windowedBy(TimeWindows.of(10000L).until(10000L))
            .count()
            .mapValues(value -> Long.toString(value))
            .toStream((key, value) -> key.key() + ";" + key.window().start())
            .to(outStream);
            ;
        */

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, props);

        //streams.cleanUp();
        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        //Thread.sleep(60000L);
        //streams.close();

        /*
        bin/kafka-console-consumer.sh --bootstrap-server 192.168.56.110:9092 --topic outEventCounts-10Sec --property print.key=true --from-beginning
        catalog_product_view    3
        catalog_product_view    1
        catalog_product_view    1
        catalog_product_view    1
        catalog_product_view    1
        catalog_product_view    1
        catalog_product_view    2
        catalog_product_view    4
        */
    }

    public static class customFilter implements Predicate<String, String>
    {
        @Override
        public boolean test(String key, String value)
        {
            if (value.toLowerCase().contains(globalJsonKey))
            {
                return true;
            }
            else
            {
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

            return retList;
        }
    }

    public static class customKVMapper implements KeyValueMapper<String, String, KeyValue<String, String>>
    {
        public KeyValue<String, String> apply(String key, String value)
        {
            return new KeyValue<String, String>(value, value);
        }
    }

    public static class customKeytoValueMapper implements KeyValueMapper<String, Long, KeyValue<String, String>>
    {
        public KeyValue<String, String> apply(String key, Long value)
        {
            return new KeyValue<String, String>("", key.toString() + " - " + value.toString());
        }
    }

    public static class groupKVMapper implements KeyValueMapper<String, String, String>
    {
        @Override
        public String apply(String key, String value) {

            //System.out.println(key.toString() + " - " + value.toString());
            return value;
        }
    }

    public static class sinkValueMapper implements ValueMapper<String, Long>
    {
        @Override
        public Long apply(String value) {

            //System.out.println(key.toString() + " - " + value.toString());
            return Long.getLong(value);
        }
    }
}
