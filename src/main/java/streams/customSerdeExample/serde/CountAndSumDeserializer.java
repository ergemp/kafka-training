package streams.customSerdeExample.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import streams.customSerdeExample.model.CountAndSum;

import java.util.Map;

public class CountAndSumDeserializer implements Deserializer<CountAndSum> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public CountAndSum deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try {
            return new CountAndSum(bytes);
        } catch (RuntimeException e) {
            throw new SerializationException("Error deserializing value", e);
        }

    }

}
