package agh.edu.pl.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkSQLAnalyzer {

	public static final String SPARK_SQL_TABLENAME = "rows";
	public static final String SPARK_SQL_VALUE_COLUMN = "value";
	public static final String SPARK_SQL_TIMESTAMP_COLUMN = "timestamp";
	
	private final String sql;
	private final List<String> tagNames;
	private ResultTypes resultType;
	private boolean isProperSql;
	private Map<String,Integer> resultIndexesMap;
	private String [] columns;
	private List<String> resultColumnNames;
	private Map<String,String> resultClassesMap;
	
	public SparkSQLAnalyzer(String sql, List<String> tagNames){
		this.sql = sql;
		this.tagNames = tagNames;
		this.isProperSql = true;
		this.resultIndexesMap = new HashMap<String, Integer>();
		this.resultColumnNames = new ArrayList<String>();
		this.resultClassesMap = new HashMap<String,String>();
	}
	
	public SparkSQLAnalyzer analyze(){
		
		String downcaseSql = sql.toLowerCase();
		
		String columnsString = downcaseSql.substring(downcaseSql.indexOf("select") + "select".length() ,downcaseSql.indexOf("from"));
		columnsString = columnsString.trim();
		
		columns = columnsString.split(",");
		
		for(int i = 0 ; i < columns.length ; i++){
			columns[i] = columns[i].trim();
			System.out.println(columns[i]);
		}
		
		
		checkIfIsProperSql();
		determineResultType();
		fillIndexesMap();
		
		
		return this;
	}
	
	
	private void fillIndexesMap() {
		for(int i = 0 ; i < resultColumnNames.size() ; i++)
			resultIndexesMap.put(resultColumnNames.get(i), new Integer(i));
		
	}

	private void determineResultType() {
		List<String> columnsAsList = new ArrayList<String>();
		for(String column : columns)
			columnsAsList.add(column);
		
		if(columnsAsList.contains("*")){
			resultType = ResultTypes.GRAPH;
			resultColumnNames.add(SPARK_SQL_TIMESTAMP_COLUMN);
			resultColumnNames.add(SPARK_SQL_VALUE_COLUMN);
			resultColumnNames.addAll(tagNames);
		} else if(columnsAsList.contains(SPARK_SQL_TIMESTAMP_COLUMN) && columnsAsList.contains(SPARK_SQL_VALUE_COLUMN)){
			resultType = ResultTypes.GRAPH;
			resultColumnNames = Arrays.asList(columns);
		} else {
			resultType = ResultTypes.DATA;
			resultColumnNames = Arrays.asList(columns);
			fillClassList(columns);
		}
	}

	private void fillClassList(String[] columnsArray) {
		for(String column : columnsArray){
			if(column.contains(SPARK_SQL_TIMESTAMP_COLUMN))
				resultClassesMap.put(column,Long.class.toString());
			else if(column.contains(SPARK_SQL_VALUE_COLUMN))
				resultClassesMap.put(column,Double.class.toString());
			else
				resultClassesMap.put(column,String.class.toString());
		}
		
	}

	private void checkIfIsProperSql() {
		for(String column : columns){
			boolean foundMatch = false;
			
			if(column.contains("*"))
				foundMatch = true;
			else if(column.contains(SPARK_SQL_VALUE_COLUMN))
				foundMatch = true;
			else if(column.contains(SPARK_SQL_TIMESTAMP_COLUMN))
				foundMatch = true;
			else {
				for(String tag : tagNames){
					if(column.contains(tag))
						foundMatch = true;
				}
			}
			
			if(!foundMatch)
				isProperSql = false;
		}
	}

	public boolean isProperSql() {
		return isProperSql;
	}

	public List<String> getResultColumnNames() {
		return resultColumnNames;
	}

	public enum ResultTypes{
		GRAPH,DATA
	}

	public ResultTypes getResultType() {
		return resultType;
	}

	public Map<String, Integer> getResultIndexesMap() {
		return resultIndexesMap;
	}

	public Map<String, String> getResultClassesMap() {
		return resultClassesMap;
	}

}
