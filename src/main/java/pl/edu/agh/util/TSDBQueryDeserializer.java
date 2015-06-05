package pl.edu.agh.util;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.edu.agh.spark.TSDBQueryParametrization;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class TSDBQueryDeserializer implements JsonDeserializer<TSDBQueryParametrization[]>{

	private static final Logger LOGGER = LogManager.getLogger(TSDBQueryDeserializer.class); 
	
	
	public TSDBQueryParametrization[] deserialize(JsonElement json, Type type, JsonDeserializationContext context) throws JsonParseException {
		
		List<TSDBQueryParametrization> queriesParametrizationList = new ArrayList<>();
		
		LOGGER.info("Deserializing json {}", json);
		
		JsonObject jsonObject = json.getAsJsonObject();
		
		JsonElement jsonStart = jsonObject.get("start");
		long start = jsonStart.getAsLong();
		
		JsonElement jsonEnd = jsonObject.get("end");
		
		long end = 0;
		if(jsonEnd == null)
			end = new Date().getTime() / 1000;
		else
			end = jsonEnd.getAsLong();
		
		if(String.valueOf(start).length() == 13)
			start /= 1000;
		if(String.valueOf(end).length() == 13)
			end /= 1000;
		
		JsonArray jsonQueriesArray = jsonObject.get("queries").getAsJsonArray();
		
		for(JsonElement jsonQuery : jsonQueriesArray){
			// many queries not supported
			JsonElement jsonQueryElement = jsonQuery;
			JsonObject jsonQueryObject = jsonQueryElement.getAsJsonObject();
			
			JsonElement jsonMetric = jsonQueryObject.get("metric");
			String metric = jsonMetric.getAsString();
			
			JsonElement jsonAggregator = jsonQueryObject.get("aggregator");
			Aggregator aggregator = Aggregators.get(jsonAggregator.getAsString());
			
			JsonElement jsonTagsElement = jsonQueryObject.get("tags");
			JsonObject jsonTagsObject = jsonTagsElement.getAsJsonObject();
			
			JsonElement jsonPossibleSqlElement = jsonQueryObject.get("sql");
			
			Map<String,String> tags = new HashMap<String,String>();
			for(Entry<String,JsonElement> entry :  jsonTagsObject.entrySet()){
				tags.put(entry.getKey(), entry.getValue().getAsString());
			}
			
			JsonElement jsonSqlElement = jsonQueryObject.get("sql");
			String sql = null;
			
			if(jsonSqlElement != null)
				sql = jsonSqlElement.getAsString();
			else if(jsonPossibleSqlElement != null){
				sql = jsonPossibleSqlElement.getAsJsonObject().getAsString();
			}
			
			TSDBQueryParametrization query = new TSDBQueryParametrization();
			query.setAggregator(aggregator);
			query.setEndTime(end);
			query.setStartTime(start);
			query.setMetric(metric);
			query.setTags(tags);
			query.setSql(sql);
			queriesParametrizationList.add(query);
		}
		
		
		return queriesParametrizationList.toArray(new TSDBQueryParametrization[queriesParametrizationList.size()]);
	}

}