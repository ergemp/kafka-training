package streams.customSerdeExample.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import streams.customSerdeExample.model.CountAndSum;

import java.util.Map;

public class CountAndSumSerializer implements Serializer<CountAndSum> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public byte[] serialize(String topic, CountAndSum countSum) {
        if (countSum == null) {
            return null;
        }

        try {
            return countSum.asByteArray();
        } catch (RuntimeException e) {
            throw new SerializationException("Error serializing value", e);
        }

    }

}
