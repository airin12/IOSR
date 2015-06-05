package pl.edu.agh.spark;

import java.util.HashMap;
import java.util.Map;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import pl.edu.agh.util.TSDBQueryDeserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class TSDBQueryParametrizationBuilder {
    private long startTime;
    private long endTime;
    private String metric;
    private Map<String, String> tags;
    private Aggregator aggregator;

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
    
    public TSDBQueryParametrizationBuilder setAggregator(Aggregator aggregator){
        this.aggregator = aggregator;
        return this;
    }

    public TSDBQueryParametrization build(){
        TSDBQueryParametrization queryParametrization = new TSDBQueryParametrization();
        queryParametrization.setEndTime(endTime);
        queryParametrization.setStartTime(startTime);
        queryParametrization.setTags(tags);
        queryParametrization.setMetric(metric);
        queryParametrization.setAggregator(aggregator);
        return queryParametrization;
    }
    
    public TSDBQueryParametrization buildFromCombinedQuery(String combinedQuery){
    	
    	String[] splittedQueryParameters = combinedQuery.split(":");   	
    	TSDBQueryParametrization queryParametrization = new TSDBQueryParametrization();
        queryParametrization.setStartTime(Long.parseLong(splittedQueryParameters[0]));
        queryParametrization.setEndTime(Long.parseLong(splittedQueryParameters[1]));
        queryParametrization.setMetric(splittedQueryParameters[2]);
        queryParametrization.setAggregator(Aggregators.get(splittedQueryParameters[3]));
        queryParametrization.setTags(buildTagsMapFromString(splittedQueryParameters[4]));
        return queryParametrization;
    }
    
    public TSDBQueryParametrization[] buildFromJson(String json){
    	GsonBuilder gsonBuilder = new GsonBuilder();
    	gsonBuilder.registerTypeAdapter(TSDBQueryParametrization[].class, new TSDBQueryDeserializer());
    	Gson gson = gsonBuilder.create();
    	
    	return gson.fromJson(json, TSDBQueryParametrization[].class);
    }
    
    private Map<String,String> buildTagsMapFromString(String tags){
    	Map<String,String> resultMap = new HashMap<String, String>();
    	
    	String [] keyValuePairs = tags.split(";");
    	
    	for(String keyValuePair : keyValuePairs){
    		String[] splittedKeyAndValue = keyValuePair.split("=");
    		if(splittedKeyAndValue.length == 2){
    			resultMap.put(splittedKeyAndValue[0], splittedKeyAndValue[1]);
    		}
    	}
    	
    	return resultMap;
    }
}
