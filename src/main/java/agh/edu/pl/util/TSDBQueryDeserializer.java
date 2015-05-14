package agh.edu.pl.util;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import agh.edu.pl.spark.TSDBQueryParametrization;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class TSDBQueryDeserializer implements JsonDeserializer<TSDBQueryParametrization>{

	private static final Logger LOGGER = LogManager.getLogger(TSDBQueryDeserializer.class); 
	
	
	public TSDBQueryParametrization deserialize(JsonElement json, Type type, JsonDeserializationContext context) throws JsonParseException {
		
		LOGGER.info("Deserializing json: {}",json.getAsString());
		
		JsonObject jsonObject = json.getAsJsonObject();
		
		JsonElement jsonStart = jsonObject.get("start");
		long start = jsonStart.getAsLong();
		
		JsonElement jsonEnd = jsonObject.get("end");
		long end = jsonEnd.getAsLong();
		
		JsonArray jsonQueriesArray = jsonObject.get("queries").getAsJsonArray();
		
		// many queries not supported
		JsonElement jsonQueryElement = jsonQueriesArray.get(0);
		JsonObject jsonQueryObject = jsonQueryElement.getAsJsonObject();
		
		JsonElement jsonMetric = jsonQueryObject.get("metric");
		String metric = jsonMetric.getAsString();
		
		JsonElement jsonAggregator = jsonQueryObject.get("aggregator");
		Aggregator aggregator = Aggregators.get(jsonAggregator.getAsString());
		
		JsonElement jsonTagsElement = jsonQueryObject.get("tags");
		JsonObject jsonTagsObject = jsonTagsElement.getAsJsonObject();
		
		Map<String,String> tags = new HashMap<String,String>();
		for(Entry<String,JsonElement> entry :  jsonTagsObject.entrySet()){
			tags.put(entry.getKey(), entry.getValue().getAsString());
		}
		
		JsonElement jsonSqlElement = jsonQueryObject.get("sql");
		String sql = jsonSqlElement.getAsString();
		
		TSDBQueryParametrization query = new TSDBQueryParametrization();
		query.setAggregator(aggregator);
		query.setEndTime(end);
		query.setStartTime(start);
		query.setMetric(metric);
		query.setTags(tags);
		query.setSql(sql);
		return query;
	}

}
