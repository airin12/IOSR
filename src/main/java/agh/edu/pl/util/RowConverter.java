package agh.edu.pl.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.spark.sql.Row;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class RowConverter {
	
	
	public String convertToJSONString(List<Row> rows, List<String> tagNames, String metric){
		
		JsonArray mainJsonArray = new JsonArray();
		Map<String,JsonObject> dataForEachTagMap = new TreeMap<String,JsonObject>();
		
		for(Row row : rows){
			Long timestamp = row.getLong(0);
			Double value = row.getDouble(1);
			List<String> tags = new ArrayList<String>();
			
			for(int i = 0; i< tagNames.size() ; i++){
				tags.add(row.getString(2+i));
			}
			
			JsonObject current = null;
			
			for(Entry<String, JsonObject> entry : dataForEachTagMap.entrySet()){
				String tagsFromMap = entry.getKey();
				if(tagsFromMap.equals(tagsAsString(tags)))
					current = entry.getValue();
			}
			
			if(current == null){
				current = new JsonObject();
				
				JsonObject tagsJsonObject = new JsonObject();
				for(int i=0 ; i<tagNames.size() ; i++){
					tagsJsonObject.addProperty(tagNames.get(i), tags.get(i));
				}
				
				current.addProperty("metric", metric);
				current.add("tags", tagsJsonObject);
				current.add("aggregateTags", new JsonArray());
				current.add("dps", new JsonObject());
				
				dataForEachTagMap.put(tagsAsString(tags), current);
			}
			
			JsonObject obj = current.get("dps").getAsJsonObject();
			obj.addProperty(timestamp.toString(), value);
			
			
		}
		
		for(Entry<String,JsonObject> entry : dataForEachTagMap.entrySet()){
			mainJsonArray.add(entry.getValue());
		}
		
		return mainJsonArray.toString();
	}

	private String tagsAsString(List<String> tags) {
		String result = "";
		for(String tag : tags)
			result+=tag;
		return result;
	}
	

}
