package pl.edu.agh.iosr.data;

import java.util.HashMap;
import java.util.Map;

public class SingleQuery {

	private String aggregator;
	private String metric;
	private String sql;
	private Map<String,String> tags = new HashMap<String,String>();
	
	public SingleQuery(){
		
	}
	
	public SingleQuery(String aggregator, String metric, String sql, Map<String, String> tags) {
		super();
		this.aggregator = aggregator;
		this.metric = metric;
		this.sql = sql;
		this.tags = tags;
	}



	public String getAggregator() {
		return aggregator;
	}
	public void setAggregator(String aggregator) {
		this.aggregator = aggregator;
	}
	public String getMetric() {
		return metric;
	}
	public void setMetric(String metric) {
		this.metric = metric;
	}
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public Map<String, String> getTags() {
		return tags;
	}
	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}
	
	
}
