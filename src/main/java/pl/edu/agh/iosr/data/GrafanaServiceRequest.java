package pl.edu.agh.iosr.data;

import java.util.ArrayList;
import java.util.List;

public class GrafanaServiceRequest {

	private long start;
	private long end;
	private List<SingleQuery> queries = new ArrayList<SingleQuery>();
	
	public GrafanaServiceRequest(long timestamp, long end, List<SingleQuery> queries) {
		super();
		this.start = timestamp;
		this.end = end;
		this.queries = queries;
	}

	public GrafanaServiceRequest(){
		
	}
	
	public long getStart() {
		return start;
	}
	public void setStart(long start) {
		this.start = start;
	}
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}
	public List<SingleQuery> getQueries() {
		return queries;
	}
	public void setQueries(List<SingleQuery> queries) {
		this.queries = queries;
	}
	
	
}
