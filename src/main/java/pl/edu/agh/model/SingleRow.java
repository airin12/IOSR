package pl.edu.agh.model;

import java.io.Serializable;
import java.util.Map;

public class SingleRow implements Serializable{
	
	private static final long serialVersionUID = -6052494625462724715L;
	
	private long timestamp;
	private double value;
	private Map<String,String> tags;
	
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
	@Override
	public String toString() {
		return "SingleRow [timestamp=" + timestamp + ", value=" + value + ", tags=" + tags + "]";
	}
	
	
}
