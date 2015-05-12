package agh.edu.pl.spark;

import java.util.List;
import java.util.Map;

public class TSDBQueryParametrizationBuilder {
    private long startTime;
    private long endTime;
    private String metric;
    private Map<String, String> tags;

    public TSDBQueryParametrizationBuilder setStartTime(long startTime){
        this.startTime = startTime;
        return this;
    }

    public TSDBQueryParametrizationBuilder setEndTime(long endTime){
        this.endTime = endTime;
        return this;
    }

    public TSDBQueryParametrizationBuilder setMetric(String metric){
        this.metric = metric;
        return this;
    }

    public TSDBQueryParametrizationBuilder setTags(Map<String, String> tags){
        this.tags = tags;
        return this;
    }

    public TSDBQueryParametrization build(){
        TSDBQueryParametrization queryParametrization = new TSDBQueryParametrization();
        queryParametrization.setEndTime(endTime);
        queryParametrization.setStartTime(startTime);
        queryParametrization.setTags(tags);
        queryParametrization.setMetric(metric);
        return queryParametrization;
    }
}
