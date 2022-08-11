package streams.customSerdeExample.actor;

import com.fasterxml.jackson.databind.ObjectMapper;
import streams.customSerdeExample.model.GenericDataModel;

import java.io.IOException;

public class MapToGenericDataModel {
    public static GenericDataModel map(String gStringModel) {
        ObjectMapper objectMapper = new ObjectMapper();
        GenericDataModel model = null;

        try {
            model = objectMapper.readValue(gStringModel, GenericDataModel.class);
        } catch (IOException e) {
            //return null;
            e.printStackTrace();
        }
        finally{
            return model;
        }
    }
}
