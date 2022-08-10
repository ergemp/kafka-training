package streams.customSerdeExample.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import streams.customSerdeExample.model.GenericDataModel;

import java.util.Map;

public class GenericDataModelSerializer implements Serializer<GenericDataModel> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public byte[] serialize(String topic, GenericDataModel genericModel) {
        if (genericModel == null) {
            return null;
        }

        try {
            return genericModel.asByteArray();
        } catch (RuntimeException e) {
            throw new SerializationException("Error serializing value", e);
        }

    }

}
