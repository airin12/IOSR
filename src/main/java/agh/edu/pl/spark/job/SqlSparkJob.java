package agh.edu.pl.spark.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.TSDB;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import agh.edu.pl.spark.SparkSQLRDDExecutor;
import agh.edu.pl.spark.TSDBQueryParametrization;
import agh.edu.pl.util.ConfigurationProvider;
import agh.edu.pl.util.RowConverter;

public class SqlSparkJob extends AbstractSparkJob {

	public SqlSparkJob(TSDB tsdb, JavaSparkContext sparkContext) {
		super(tsdb, sparkContext);
	}

	@Override
	protected Object execute(JavaRDD<Double> rdd) {
		return "";
	}

	private Object executeSQLQuery(JavaPairRDD<Long,Long> rdd, String sql, SQLContext sqlContext, String metric, List<String> tagNames, String combinedQuery) {
		
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("timestamp", DataTypes.LongType, true));
		fields.add(DataTypes.createStructField("value", DataTypes.DoubleType, true));
		for(String tag : tagNames)
			fields.add(DataTypes.createStructField(tag, DataTypes.StringType, true));
		
		StructType schema = DataTypes.createStructType(fields);
		
		SparkSQLRDDExecutor executor = new SparkSQLRDDExecutor();
		JavaRDD<Row> rowRDD = executor.loadTSDBData(rdd,combinedQuery);

		DataFrame rowsDataFrame = sqlContext.createDataFrame(rowRDD, schema);
		rowsDataFrame.registerTempTable("rows");
		
		DataFrame results = sqlContext.sql(sql);
		JavaRDD<Row> resultRows = results.javaRDD();
		List<Row> valuesInList = resultRows.collect();
		
		RowConverter converter = new RowConverter();

		return converter.convertToJSONString(valuesInList, tagNames, metric);
	}

	@Override
	public Object execute(TSDBQueryParametrization queryParametrization) {
		SQLContext sqlContext = new SQLContext(sparkContext);
		
		ConfigurationProvider configProvider;
		int numSlices = 1;
		
		try {
			configProvider = new ConfigurationProvider(ConfigurationProvider.CONFIGURATION_FILENAME);
			numSlices = Integer.parseInt(configProvider.getProperty(ConfigurationProvider.SPARK_SLAVES_NUMBER_PROPERTY_NAME));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		List<Tuple2<Long,Long>> timestamps = generateTimestampsList(queryParametrization.getStartTime(),queryParametrization.getEndTime(), numSlices);
		
		return executeSQLQuery(sparkContext.parallelizePairs(timestamps, numSlices),queryParametrization.getSql(), sqlContext, queryParametrization.getMetric(),generateTagsListFromMap(queryParametrization.getTags()),queryParametrization.toCombinedQuery());
	}
	
	private List<Tuple2<Long, Long>> generateTimestampsList(long startTime, long endTime, int slices) {
		List<Tuple2<Long, Long>> timestamps = new ArrayList<Tuple2<Long, Long>>();
		
		long diff = (endTime - startTime) / slices;
		long actualTimestamp = startTime;
		
		while(actualTimestamp < endTime){
			Long start = new Long(actualTimestamp);
			Long end;
			
			if(actualTimestamp + diff > endTime)
				end = new Long(endTime);
			else
				end = new Long(actualTimestamp + diff);
			
			actualTimestamp += diff + 1;
			
			timestamps.add(new Tuple2<Long, Long>(start, end));
		}
		
		return timestamps;
	}

	private List<String> generateTagsListFromMap(Map<String,String> map){
		Object [] tagsArray =  map.keySet().toArray();
		List<String> tagsList = new ArrayList<String>();
		
		for(Object tag : tagsArray)
			tagsList.add(tag.toString());
		
		return tagsList;
	}

}