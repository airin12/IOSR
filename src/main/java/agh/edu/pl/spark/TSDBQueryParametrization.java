package agh.edu.pl.spark;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

import net.opentsdb.core.Aggregator;

public class TSDBQueryParametrization implements Serializable{
	private static final long serialVersionUID = 1L;
	private long startTime;
    private long endTime;
    private String metric;
    private Map<String, String> tags;
    private Aggregator aggregator;
    private String sql;

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

	public Aggregator getAggregator() {
		return aggregator;
	}

	public void setAggregator(Aggregator aggregator) {
		this.aggregator = aggregator;
	}
	
	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	@Override
	public String toString() {
		return "TSDBQueryParametrization [startTime=" + startTime + ", endTime=" + endTime + ", metric=" + metric + ", tags=" + tags + ", aggregator="
				+ aggregator + "]";
	}
    
	public String toCombinedQuery(){
		String combinedQuery = startTime+":"+endTime+":"+metric+":"+aggregator.toString()+":";
		boolean isFirst = true;
		
		for(Entry<String,String> entry : tags.entrySet()){
			if(isFirst){
				isFirst = false;
			} else {
				combinedQuery += ";";
			}
			
			combinedQuery += entry.getKey()+"="+entry.getValue();
				
		}
		
		return combinedQuery;
	}
	
}
