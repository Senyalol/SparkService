package spark;

import java.io.Serializable;

/**
 * Событие RFM-сегмента для записи в топик user-segments.
 */
public class SegmentEvent implements Serializable {
    private int user_id;
    private String segment;
    private double r_minutes;
    private long f;
    private double m;
    private long updated_at;

    public int getUser_id() { return user_id; }
    public void setUser_id(int user_id) { this.user_id = user_id; }
    public String getSegment() { return segment; }
    public void setSegment(String segment) { this.segment = segment; }
    public double getR_minutes() { return r_minutes; }
    public void setR_minutes(double r_minutes) { this.r_minutes = r_minutes; }
    public long getF() { return f; }
    public void setF(long f) { this.f = f; }
    public double getM() { return m; }
    public void setM(double m) { this.m = m; }
    public long getUpdated_at() { return updated_at; }
    public void setUpdated_at(long updated_at) { this.updated_at = updated_at; }
}
