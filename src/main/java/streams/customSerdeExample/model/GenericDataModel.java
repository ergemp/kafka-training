package streams.customSerdeExample.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class GenericDataModel {
    private String token;
    private Long ts;
    private String label;
    private String payload;
    private String payload2;

    public GenericDataModel(String gToken, Long gTs, String gLabel, String gPayload, String gPayload2) {
        this.token = gToken;
        this.ts = gTs;
        this.label = gLabel;
        this.payload = gPayload;
        this.payload2 = gPayload2;
    }

    public GenericDataModel() {

    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getPayload() {
        return payload;
    }

    public Double getPayloadasDouble() {
        return Double.valueOf(payload);
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getPayload2() {return payload2;}

    public void setPayload2(String payload2) { this.payload2 = payload2; }

    public GenericDataModel(String gStringModel) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.ts = objectMapper.readValue(gStringModel, GenericDataModel.class).getTs();
            this.payload = objectMapper.readValue(gStringModel, GenericDataModel.class).getPayload();
            this.payload2 = objectMapper.readValue(gStringModel, GenericDataModel.class).getPayload2();
            this.label = objectMapper.readValue(gStringModel, GenericDataModel.class).getLabel();
            this.token = objectMapper.readValue(gStringModel, GenericDataModel.class).getToken();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public GenericDataModel(byte[] bytes) {
        this(new String(bytes));
    }

    public byte[] asByteArray() {
        return toString().getBytes(StandardCharsets.UTF_8);
    }

    public String toString() {
        ObjectMapper objectMapper = new ObjectMapper();
        String retVal = "";
        try {
            retVal = objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        finally {
            return retVal;
        }
    }
}
