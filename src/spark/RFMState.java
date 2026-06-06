package spark;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class RFMState {
    @JsonProperty("lastTs")
    private long lastTs;
    @JsonProperty("firstTs")
    private long firstTs;
    @JsonProperty("lastWallMs")
    private long lastWallMs;
    @JsonProperty("entries")
    private List<TransactionEntry> entries = new ArrayList<>();
    @JsonProperty("fWindow")
    private long fWindow;
    @JsonProperty("fTotal")
    private long fTotal;
    @JsonProperty("mWindow")
    private double mWindow;
    @JsonProperty("mTotal")
    private double mTotal;
    @JsonProperty("rMinutes")
    private double rMinutes;

    public RFMState() {}

    // Геттеры и сеттеры
    public long getLastTs() { return lastTs; }
    public void setLastTs(long lastTs) { this.lastTs = lastTs; }

    public long getFirstTs() { return firstTs; }
    public void setFirstTs(long firstTs) { this.firstTs = firstTs; }

    public long getLastWallMs() { return lastWallMs; }
    public void setLastWallMs(long lastWallMs) { this.lastWallMs = lastWallMs; }

    public List<TransactionEntry> getEntries() { return entries; }
    public void setEntries(List<TransactionEntry> entries) { this.entries = entries; }

    public long getFWindow() { return fWindow; }
    public void setFWindow(long fWindow) { this.fWindow = fWindow; }

    public long getFTotal() { return fTotal; }
    public void setFTotal(long fTotal) { this.fTotal = fTotal; }

    public double getMWindow() { return mWindow; }
    public void setMWindow(double mWindow) { this.mWindow = mWindow; }

    public double getMTotal() { return mTotal; }
    public void setMTotal(double mTotal) { this.mTotal = mTotal; }

    // Для обратной совместимости
    public long getF() { return fWindow; }
    public void setF(long f) { this.fWindow = f; }

    public double getM() { return mWindow; }
    public void setM(double m) { this.mWindow = m; }

    public double getRMinutes() { return rMinutes; }
    public void setRMinutes(double rMinutes) { this.rMinutes = rMinutes; }

}