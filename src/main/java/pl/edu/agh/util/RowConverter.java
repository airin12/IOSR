package pl.edu.agh.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.spark.sql.Row;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class RowConverter {
	
	
	public JsonElement convertToJSONElement(List<Row> rows, List<String> tagNames, String metric, SparkSQLAnalyzer analyzer, JsonElement result){
		
		if(analyzer.getResultType().equals(SparkSQLAnalyzer.ResultTypes.GRAPH)){
			JsonArray mainJsonArray = result.getAsJsonArray();
			Map<String,JsonObject> dataForEachTagMap = new TreeMap<String,JsonObject>();
			Map<String,Integer> indexes = analyzer.getResultIndexesMap();
			
			for(Row row : rows){
				Long timestamp = row.getLong(indexes.get(SparkSQLAnalyzer.SPARK_SQL_TIMESTAMP_COLUMN));
				
				if(timestamp.toString().length() == 13)
					timestamp /= 1000;
				
				Double value = row.getDouble(indexes.get(SparkSQLAnalyzer.SPARK_SQL_VALUE_COLUMN));
				List<String> tags = new ArrayList<String>();
				
				for(int i = 0; i< analyzer.getResultColumnNames().size() - 2 ; i++){
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
					for(int i=0 ; i< analyzer.getResultColumnNames().size() - 2 ; i++){
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
			
			return mainJsonArray;
			
		} else {
			JsonObject dpsObject = new JsonObject();
			
			JsonObject newQueryResult = new JsonObject();
			newQueryResult.addProperty("metric", metric);
			newQueryResult.add("tags", new JsonObject());
			newQueryResult.add("aggregateTags", new JsonArray());
			newQueryResult.add("dps", dpsObject);
			
			if(rows.size() > 0)
				dpsObject.addProperty("-1", -1);
			
			List<String> columns = analyzer.getResultColumnNames();
			Map<String,Integer> indexes = analyzer.getResultIndexesMap();
			Map<String,String> classes = analyzer.getResultClassesMap();
			
			for(Row row : rows){
				for(String column : columns){
					if(classes.get(column).equals(Long.class.toString()))
						dpsObject.addProperty(column, row.getLong(indexes.get(column)));
					else if(classes.get(column).equals(Double.class.toString()))
						dpsObject.addProperty(column, row.getDouble(indexes.get(column)));
					else
						dpsObject.addProperty(column, row.getString(indexes.get(column)));
				}
			}
			
			JsonArray newResult = result.getAsJsonArray();
			newResult.add(newQueryResult);
			return newResult;
		}
	}

	private String tagsAsString(List<String> tags) {
		String result = "";
		for(String tag : tags)
			result+=tag;
		return result;
	}
	

}
