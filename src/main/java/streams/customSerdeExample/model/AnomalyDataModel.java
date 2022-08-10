package streams.customSerdeExample.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AnomalyDataModel {
    private String token;
    private Long ts;
    private String label;
    private Double payload;
    private String payload2;
    private Double anomaly;

    public AnomalyDataModel(String gToken, Long gTs, String gLabel, Double gPayload, String gPayload2) {
        this.token = gToken;
        this.ts = gTs;
        this.label = gLabel;
        this.payload = gPayload;
        this.payload2 = gPayload2;
        this.anomaly = 0.0;
    }

    public AnomalyDataModel() {

    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Double getAnomaly() {
        return anomaly;
    }

    public void setAnomaly(Double anomaly) {
        this.anomaly = anomaly;
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

    public Double getPayload() {
        return payload;
    }

    public void setPayload(Double payload) {
        this.payload = payload;
    }

    public String getPayload2() {return payload2;}

    public void setPayload2(String payload2) { this.payload2 = payload2; }

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
