package streams.customSerdeExample.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties
public class CountAndSum {
    @JsonProperty("count")
    private Integer count;
    @JsonProperty("sum")
    private Double sum;
    @JsonProperty("valueList")
    private List<Double> valueList = new ArrayList<>();
    @JsonProperty("valueObjectList")
    private List<String> valueObjectList = new ArrayList<>();
    @JsonProperty("deviation")
    private Double deviation;

    public CountAndSum(Double sum, Integer count, Double deviation) {
        this.count = count;
        this.sum = sum;
        this.deviation = 0D;
    }

    public void addValue(Double gValue){
        valueList.add(gValue);
    }

    public void addValue(String gValueObject){
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            JsonNode jsonNode = objectMapper.readTree(gValueObject);
            addValue(Double.valueOf(jsonNode.get("payload").asText()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addValueObject(String gValue){
        valueObjectList.add(gValue);
    }

    public CountAndSum() {
        //No Creators, like default construct, exist): cannot deserialize from Object value (no delegate- or property-based Creator

        //This error occurs because jackson library doesn't know how to create your model which doesn't have empty constructor
        //and the model contains constructor with parameters which didn't annotated its parameters with @JsonProperty("field_name").
        //By default java compiler creates empty constructor if you didn't add constructor to your class

        //Add empty constructor to your model or annotate constructor parameters with @JsonProperty("field_name")
    }

    public CountAndSum(String gStringModel) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.sum = objectMapper.readValue(gStringModel, CountAndSum.class).getSum();
            this.count = objectMapper.readValue(gStringModel, CountAndSum.class).getCount();
            this.valueList = objectMapper.readValue(gStringModel, CountAndSum.class).getValues();
            this.valueObjectList = objectMapper.readValue(gStringModel,CountAndSum.class).getValueObjects();
            this.deviation = objectMapper.readValue(gStringModel, CountAndSum.class).getDeviation();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CountAndSum(byte[] bytes) {
        this(new String(bytes));
    }

    public List<Double> getValues() {
        return this.valueList;
    }

    public List<String> getValueObjects() {
        return this.valueObjectList;
    }

    public void setValues(List<Double> valueList) {
        this.valueList = valueList;
    }

    public void setValueObjects(List<String> valueObjectList) {
        this.valueObjectList = valueObjectList;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    public byte[] asByteArray() {
        return toString().getBytes(StandardCharsets.UTF_8);
    }

    public Double calculateDeviation(){
        Double deviation = 0.0;

        for (Double val : this.getValues()) {
            deviation += Math.pow(val - (this.sum/this.count), 2);
        }
        deviation = Math.sqrt(deviation/count);

        this.deviation = deviation;

        return deviation;
    }


    public void updateRecDeviationWithValue(Double value) {
        if (this.count == 1) return;
        Double avg = this.sum / this.count;
        Double prevVariation = (Math.pow(this.deviation, 2)) * (this.count - 1);
        Double newVariation = prevVariation + Math.pow(value - avg, 2);
        this.deviation = Math.sqrt(newVariation / this.count);
    }

    public Double getDeviation(){
        return this.deviation;
    }

    public void setDeviation(Double deviation){
        this.deviation = deviation;
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
