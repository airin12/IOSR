package agh.edu.pl.spark;

import net.opentsdb.core.Aggregator;

import java.util.List;
import java.util.Map;

public class TSDBQueryParametrization {
    private long startTime;
    private long endTime;
    private String metric;
    private Map<String, String> tags;

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}
