package pl.edu.agh.iosr.config;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import pl.edu.agh.iosr.generator.GeneratorWorkModes;

public class Configuration {
	private String grafanaServiceAddress = null;
	private String tsdbServiceAddress = null;
	private String metric = null;
	private Map<String,String> tags = new HashMap<String, String>();
	private GeneratorWorkModes mode = null;
	private String file = null;
	private long delay = 1000;
	private String format = null;
	private boolean isValid = true;
	private Map<String,Integer> columnMap = new HashMap<String, Integer>();
	private int numberOfRequests = 0;
	private double min = 0.0;
	private double max = 0.0;
	private String errorMsg;
	private int duplicate = 1;
	private String separator;
	private List<String> tagsNamesFromFile = new ArrayList<String>();
	private long start;
	private long end;
	private String aggregator;
	private int timeStep;
	private String sql;
	
	private static final String METRIC_ARG = "metric";
	private static final String MODE_ARG = "mode";
	private static final String SERVICE_ADDRESS_ARG = "service_address";
	private static final String TSDB_ADDRESS_ARG = "tsdb_address";
	private static final String TAGS_ARG = "tags";
	private static final String REQUESTS_ARG = "req_nr";
	private static final String DELAY_ARG = "delay";
	private static final String MAX_ARG = "max";
	private static final String MIN_ARG = "min";
	private static final String DUPLICATE_ARG = "duplicate";
	private static final String FILE_ARG = "file";
	private static final String SEP_ARG = "separator";
	private static final String FORMAT_ARG = "format";
	private static final String START_ARG = "start";
	private static final String END_ARG = "end";
	private static final String AGR_ARG = "aggregator";
	private static final String STEP_ARG = "step";
	private static final String SQL_ARG = "sql";
	
	public static final String TIMESTAMP_COL = "timestamp";
	public static final String VALUE_COL = "value";
	
	
	public Configuration(String [] args){
		String modeString = getArg(args, MODE_ARG);
		try{
			mode = GeneratorWorkModes.valueOf(modeString);
		} catch (Exception ex){
			isValid = false;
			errorMsg = "You must specify "+MODE_ARG+" parameter";
			return;
		}
		
		metric = getArg(args, METRIC_ARG);
		if(metric == null){
			isValid = false;
			errorMsg = "You must specify "+METRIC_ARG+" parameter";
			return;
		}
		
		grafanaServiceAddress = getArg(args, SERVICE_ADDRESS_ARG);
		if(grafanaServiceAddress == null && (mode.equals(GeneratorWorkModes.TEST1) || mode.equals(GeneratorWorkModes.TEST2) || mode.equals(GeneratorWorkModes.TEST3))){
			isValid = false;
			errorMsg = "You must specify "+SERVICE_ADDRESS_ARG+" parameter";
			return;
		}
		
		tsdbServiceAddress = getArg(args, TSDB_ADDRESS_ARG);
		if(tsdbServiceAddress == null && (mode.equals(GeneratorWorkModes.GENERATE) || mode.equals(GeneratorWorkModes.LOAD) || mode.equals(GeneratorWorkModes.TEST3))){
			isValid = false;
			errorMsg = "You must specify "+TSDB_ADDRESS_ARG+" parameter";
			return;
		}
		
		String tags = getArg(args, TAGS_ARG);
		if(tags == null && !mode.equals(GeneratorWorkModes.LOAD)){
			isValid = false;
			errorMsg = "You must specify "+TAGS_ARG+" parameter";
			return;
		} else if(tags != null){
			generateTags(tags);
		}
		
		String requests = getArg(args, REQUESTS_ARG);
		try{
			numberOfRequests = Integer.parseInt(requests);
		} catch (Exception ex){
			if(!mode.equals(GeneratorWorkModes.LOAD)){
				isValid = false;
				errorMsg = "You must specify "+REQUESTS_ARG+" parameter";
				return;
			}
		}
		
		String delayString = getArg(args, DELAY_ARG);
		try{
			delay = Long.parseLong(delayString);
		} catch (Exception ex){
			delay = 1000;
		}
		
		String maxString = getArg(args, MAX_ARG);
		try{
			max = Double.parseDouble(maxString);
		} catch (Exception ex){
			max = 100.0;
		}
		
		String minString = getArg(args, MIN_ARG);
		try{
			min = Double.parseDouble(minString);
		} catch (Exception ex){
			min = 0.0;
		}
		
		String dupString = getArg(args, DUPLICATE_ARG);
		try{
			duplicate = Integer.parseInt(dupString);
		} catch (Exception ex){
			duplicate = 1;
		}
		
		file = getArg(args, FILE_ARG);
		if(file == null && !mode.equals(GeneratorWorkModes.GENERATE)){
			isValid = false;
			errorMsg = "You must specify "+FILE_ARG+" parameter";
			return;
		}
		
		separator = getArg(args, SEP_ARG);
		if(separator == null && mode.equals(GeneratorWorkModes.LOAD)){
			isValid = false;
			errorMsg = "You must specify "+SEP_ARG+" parameter";
			return;
		}
		
		format = getArg(args, FORMAT_ARG);
		if(format == null && mode.equals(GeneratorWorkModes.LOAD)){
			isValid = false;
			errorMsg = "You must specify "+FORMAT_ARG+" parameter";
			return;
		} else if (format != null){
			initializeDataFromFormat();
		}
		
		String startString = getArg(args, START_ARG);
		try{
			start = Long.parseLong(startString);
		} catch (Exception ex){
			start = new Date().getTime() - 1;
		}
		
		String endString = getArg(args, END_ARG);
		try{
			end = Long.parseLong(endString);
		} catch (Exception ex){
			end = new Date().getTime();
		}
		
		aggregator = getArg(args, AGR_ARG);
		if(aggregator == null && (mode.equals(GeneratorWorkModes.TEST1) || mode.equals(GeneratorWorkModes.TEST2))){
			isValid = false;
			errorMsg = "You must specify "+AGR_ARG+" parameter";
			return;
		}
		
		String timeStepString = getArg(args, STEP_ARG);
		try{
			timeStep = Integer.parseInt(timeStepString);
		} catch (Exception ex){
			timeStep = 0;
		}
		
		sql = getArg(args, SQL_ARG);
		if(sql == null && mode.equals(GeneratorWorkModes.TEST2)){
			isValid = false;
			errorMsg = "You must specify "+SQL_ARG+" parameter";
			return;
		} else if (sql != null){
			sql = sql.replace(";", " ");
		}
	}

	private void initializeDataFromFormat() {
		String[] columns = format.split(separator);
		for(int i = 0 ; i < columns.length ; i++){
			if(columns[i].equals(TIMESTAMP_COL)){
				columnMap.put(TIMESTAMP_COL, new Integer(i));
			} else if (columns[i].equals(VALUE_COL)){
				columnMap.put(VALUE_COL, new Integer(i));
			} else if (columns[i].length() > 0){
				tagsNamesFromFile.add(columns[i]);
				columnMap.put(columns[i], new Integer(i));
			}
		}
		
	}

	private void generateTags(String tagsString) {
		String [] tagPairs = tagsString.split(",");
		for(String tagPair : tagPairs){
			String [] tagKeyValue = tagPair.split(":");
			if(tagKeyValue.length == 2){
				this.tags.put(tagKeyValue[0], tagKeyValue[1]);
			}
		}
	}

	public boolean isValid() {
		return isValid;
	}
	
	private String getArg(String [] args, String argName){
		for(String arg : args){
			if(arg.startsWith(argName+"="))
				return arg.split("=")[1];
		}
		
		return null;
	}

	public String getGrafanaServiceAddress() {
		return grafanaServiceAddress;
	}

	public String getMetric() {
		return metric;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public GeneratorWorkModes getMode() {
		return mode;
	}

	public String getFile() {
		return file;
	}


	public long getDelay() {
		return delay;
	}

	public String getFormat() {
		return format;
	}

	public Map<String, Integer> getColumnMap() {
		return columnMap;
	}

	public int getNumberOfRequests() {
		return numberOfRequests;
	}

	public double getMin() {
		return min;
	}

	public double getMax() {
		return max;
	}

	public String getErrorMsg() {
		return errorMsg;
	}

	public int getDuplicate() {
		return duplicate;
	}

	public String getSeparator() {
		return separator;
	}

	public List<String> getTagsNamesFromFile() {
		return tagsNamesFromFile;
	}

	public long getStart() {
		return start;
	}

	public long getEnd() {
		return end;
	}

	public String getAggregator() {
		return aggregator;
	}

	public int getTimeStep() {
		return timeStep;
	}

	public String getSql() {
		return sql;
	}

	public String getTsdbServiceAddress() {
		return tsdbServiceAddress;
	}

	
}
