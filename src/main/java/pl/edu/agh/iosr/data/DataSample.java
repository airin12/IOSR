package pl.edu.agh.iosr.data;

import java.util.Map;

public class DataSample {

	private String metric;
	private long timestamp;
	private double value;
	private Map<String,String> tags;
	
	public DataSample(String metric, long timestamp, double value,
			Map<String, String> tags) {
		super();
		this.metric = metric;
		this.timestamp = timestamp;
		this.value = value;
		this.tags = tags;
	}
	
	public DataSample(){
		
	}

	public String getMetric() {
		return metric;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}
	
	
	
}
