package spark;

import java.io.Serializable;

/**
 * Событие аномалии для записи в топик alerts.
 */
public class AlertEvent implements Serializable {
    private int user_id;
    private long event_time;
    private String type;
    private double sum;
    private double avg_check_5min;
    private String message;

    public int getUser_id() { return user_id; }
    public void setUser_id(int user_id) { this.user_id = user_id; }
    public long getEvent_time() { return event_time; }
    public void setEvent_time(long event_time) { this.event_time = event_time; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public double getSum() { return sum; }
    public void setSum(double sum) { this.sum = sum; }
    public double getAvg_check_5min() { return avg_check_5min; }
    public void setAvg_check_5min(double avg_check_5min) { this.avg_check_5min = avg_check_5min; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}
