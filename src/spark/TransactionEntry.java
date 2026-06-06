package spark;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TransactionEntry {
    @JsonProperty("timestamp")
    private long timestamp;
    @JsonProperty("sum")
    private double sum;
    @JsonProperty("type")
    private String type;

    public TransactionEntry() {}

    public TransactionEntry(long timestamp, double sum, String type) {
        this.timestamp = timestamp;
        this.sum = sum;
        this.type = type;
    }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public double getSum() { return sum; }
    public void setSum(double sum) { this.sum = sum; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
}
