package streams;

import org.apache.commons.cli.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class UniqueEventCounter {
    private static String globalJsonKey = "pagetype";
    private static String leftStream = "test";
    private static String lookupTable = "outTopics";
    private static String appID = "streamingAPI-unqEventProcessor";
    private static String bootstrapServers = "localhost:9092";

    public static void main(String[] args) throws Exception
    {
        Options options = new Options();
        options.addOption("globalJsonKey", true, "json key for the event value");
        options.addOption("leftStream", true, "kafka topic for streaming events");
        options.addOption("lookupTable", true, "kafka topic name for lookup. This topic will stores unique events");
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
                leftStream = cmd.getOptionValue("leftStream");
            }

            if (cmd.hasOption("lookupTable"))
            {
                lookupTable = cmd.getOptionValue("lookupTable");
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
            System.out.println("leftStream: " + leftStream + " --> Description: " + options.getOption("leftStream").getDescription());
            System.out.println("lookupTable: " + lookupTable + " --> Description: " + options.getOption("lookupTable").getDescription());
            System.out.println("appID: " + appID + " --> Description: " + options.getOption("appID").getDescription());
            System.out.println("bootstrapServers: " + bootstrapServers + " --> Description: " + options.getOption("bootstrapServers").getDescription());
            System.out.println("-----------------------");
        }


        //System.exit(0);

        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();
        final Serde <Long> longSerde = Serdes.Long();

        System.out.println("Main: creating properties with the supplied parameters");
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
        KStream<String, String> source = builder.stream(leftStream);
        KTable<String, String> unqTopics = builder.table(lookupTable);

        KStream<String, String> events = source
                .filter(new jsonKeyValueFilter())
                .flatMapValues(new jsonValueMapper())
                .map(new customVKMapper())
                ;

        //KStream<String, String> joined =
        events
                .leftJoin(unqTopics, new customValueJoiner())
                .filter(new checkNullRightFilter())
                .map(new customKVMapper())
                .to(lookupTable)
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
        // Thread.sleep(60000L);
        // streams.close();
    }

    public static class checkNullRightFilter implements Predicate<String, String>
    {
        @Override
        public boolean test(String key, String value)
        {
            //System.out.println("checkNullRightFilter -- key: " + key + " - value: " + value);
            if (value == null)
            {
                System.out.println("checkNullRightFilter: new event captured --> (key: " + key + " - value: " + value + ")");
                return true;
            }
            else
            {
                return false;
            }
        }
    }

    public static class jsonKeyValueFilter implements Predicate<String, String>
    {
        @Override
        public boolean test(String key, String value)
        {
            //System.out.println("jsonKeyValueFilter -- key: " + key + " - value: " + value);
            if (value.toLowerCase().contains(globalJsonKey.toLowerCase()))
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }

    public static class jsonValueMapper implements ValueMapper<String, Iterable<String>>
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
                if (arrVal[i].toLowerCase().contains(globalJsonKey.toLowerCase()))
                {
                    retStr = arrVal[i].split(":")[1];
                }
            }

            List<String> retList = new ArrayList<>();
            retList.add(retStr);

            return retList;
        }
    }

    public static class customVKMapper implements KeyValueMapper<String, String, KeyValue<String, String>>
    {
        public KeyValue<String, String> apply(String key, String value)
        {
            //System.out.println("customVKMapper -- key: " + key + " - value: " + value);
            return new KeyValue<String, String>(value, value);
        }
    }

    public static class customKVMapper implements KeyValueMapper<String, String, KeyValue<String, String>>
    {
        public KeyValue<String, String> apply(String key, String value)
        {
            //System.out.println("customKVMapper -- key: " + key + " - value: " + value);
            return new KeyValue<String, String>(key, key);
        }
    }

    public static class customValueJoiner implements ValueJoiner<String, String, String>
    {
        @Override
        public String apply(String leftValue, String rightValue)
        {
            return rightValue;

        }
    }
}
