package streams.customSerdeExample.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import streams.customSerdeExample.model.GenericDataModel;

import java.util.Map;

public class GenericDataModelDeserializer implements Deserializer<GenericDataModel> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public GenericDataModel deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try {
            return new GenericDataModel(bytes);
        } catch (RuntimeException e) {
            throw new SerializationException("Error deserializing value", e);
        }

    }

}
