package streams.customSerdeExample.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import streams.customSerdeExample.actor.MapToGenericDataModel;
import streams.customSerdeExample.model.AnomalyDataModel;
import streams.customSerdeExample.model.CountAndSum;
import streams.customSerdeExample.serde.CountAndSumSerializer;
import streams.customSerdeExample.serde.CountAndSumDeserializer;
import streams.customSerdeExample.serde.GenericDataModelSerializer;
import streams.customSerdeExample.serde.GenericDataModelDeserializer;
import streams.customSerdeExample.model.GenericDataModel;
import streams.customSerdeExample.util.GetStreamsProperties;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class LocalAverage {
    public static void runStream(String gToken, String gApplicationName) {
        //get the streams properties
        //pass the applicationName to the properties
        Properties props = GetStreamsProperties.get(gApplicationName);

        //build the streams
        //token is the topic name to consume
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(gToken);

        //create custom serde
        Serializer<CountAndSum> CountAndSumSerializer = new CountAndSumSerializer();
        Deserializer<CountAndSum> CountAndSumDeserializer = new CountAndSumDeserializer();
        Serde<CountAndSum> CountAndSumSerde = Serdes.serdeFrom(CountAndSumSerializer, CountAndSumDeserializer);

        Serializer<GenericDataModel> GenericDataModelSerializer = new GenericDataModelSerializer();
        Deserializer<GenericDataModel> GenericDataModelDeserializer = new GenericDataModelDeserializer();
        Serde<GenericDataModel> GenericDataModelSerde = Serdes.serdeFrom(GenericDataModelSerializer, GenericDataModelDeserializer);

        //streams topology
        /*
        source
                .selectKey((key,value) -> "key")
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                .count()
                .toStream()
                .foreach((key, value) -> System.out.println(key + " " + value))
        ;
        */

        TimeWindowedKStream<String, String> windowedStream =
                stream.mapValues(MapToGenericDataModel::map)
                        //.map((key, value) -> KeyValue.pair(value.getLabel(), value.getPayload() + "," + value.getLabel() + "," + value.getTs() ) )
                        .map((key, value) -> KeyValue.pair(value.getLabel(), value.toString()) )
                        //.mapValues(GenericDataModel::getPayload)
                        //.selectKey((key, value) -> "key")
                        .groupByKey()
                        .windowedBy(TimeWindows.of(Duration.ofSeconds(60)))
                ;

        AtomicReference<Long> tt = null;
        ObjectMapper objectMapper = new ObjectMapper();

        KStream<Windowed<String>, CountAndSum> avgStream =
                windowedStream
                        .aggregate(
                                () -> new CountAndSum(0D,0,0D),
                                (key, value, aggregate) -> {
                                    aggregate.setCount(aggregate.getCount() + 1);
                                    try {
                                        aggregate.setSum(aggregate.getSum() + objectMapper.readTree(value).get("payload").asDouble());
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                    aggregate.addValueObject(value);
                                    aggregate.addValue(value);
                                    return aggregate;
                                }, Materialized.with(Serdes.String(), CountAndSumSerde)
                        )
                        .toStream()
                        .peek((key, value) -> System.out.println("Sum: " + value.getSum() + " Count: " + value.getCount() + " Avg: " + value.getSum()/value.getCount() +  " Deviation: " +  value.calculateDeviation() + " Values: " + value.getValues().toString() ))
                //.peek((key, value) -> value.calculateDeviation())
                ;

        avgStream
                .map((key, value) -> new KeyValue<String, CountAndSum>(key.key(),value))
                .flatMap((key, value) -> {

                    List<KeyValue<String, AnomalyDataModel>> ll = new LinkedList<>();
                    //ObjectMapper objectMapper = new ObjectMapper();

                    for (String val : value.getValueObjects()) {
                        AnomalyDataModel aModel = new AnomalyDataModel();
                        Double avg = value.getSum()/value.getCount();

                        System.out.println(val);

                        try {
                            if (Math.abs(objectMapper.readTree(val).get("payload").asLong() - avg) <= value.getDeviation() * 2 ) {
                                aModel.setAnomaly(0.0);
                            }
                            else {
                                aModel.setAnomaly(1.0);
                                aModel.setAnomaly(objectMapper.readTree(val).get("payload").asDouble());
                            }

                            aModel.setPayload(objectMapper.readTree(val).get("payload").asDouble());
                            aModel.setLabel(objectMapper.readTree(val).get("label").asText());
                            aModel.setTs(objectMapper.readTree(val).get("ts").asLong());

                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        ll.add(KeyValue.pair(key,aModel));
                    }
                    return ll;
                })
                .map((key,value) -> {

                    String jsonString = "";

                    jsonString +="{";
                    jsonString +="\"label\":\"" + value.getLabel() + "\",";
                    jsonString +="\"value\":" + value.getPayload() +",";
                    jsonString +="\"anomaly\":" + value.getAnomaly() +",";
                    jsonString +="\"ts\":\"" + value.getTs() +"\"";
                    jsonString +="}";

                    return KeyValue.pair(key,jsonString);
                })
                //.peek((key,value) -> System.out.println("key: " + key + " value: " + value))
                .to(gToken +"-LAVG")
        //.foreach((key,value) -> System.out.println("key: " + key + " value: " + value))
        ;

        //build the topology
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        //cleanUp and start the streams application
        streams.cleanUp();
        streams.start();
    }
}
