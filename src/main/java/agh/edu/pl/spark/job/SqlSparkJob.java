package agh.edu.pl.spark.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import agh.edu.pl.spark.SparkSQLRDDExecutor;
import agh.edu.pl.spark.TSDBQueryParametrization;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import agh.edu.pl.model.SingleRow;
import agh.edu.pl.util.ConfigurationProvider;
import agh.edu.pl.util.DataPointsConverter;
import agh.edu.pl.util.RowConverter;

public class SqlSparkJob extends AbstractSparkJob {

	private static final Logger LOGGER = LogManager.getLogger(SqlSparkJob.class);

	public SqlSparkJob(TSDB tsdb, JavaSparkContext sparkContext) {
		super(tsdb, sparkContext);
	}

	@Override
	protected Object execute(JavaRDD<Double> rdd) {
		return "";
	}

	private Object executeSQLQuery(JavaRDD<SingleRow> rdd, String sql, SQLContext sqlContext, String metric, List<String> tagNames) {
		String schemaString = "timestamp value cpu";

		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(" ")) {
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		
		SparkSQLRDDExecutor executor = new SparkSQLRDDExecutor();
		JavaRDD<Row> rowRDD = executor.execute(rdd);

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
		Query query = buildQuery(queryParametrization);
		DataPoints[] matchingPoints = query.run();
		if (matchingPoints.length == 0) {
			LOGGER.info("No matching points for query found. Returning 0.0");
			return 0.0;
		}

		DataPointsConverter parser = new DataPointsConverter();
		List<SingleRow> rows = parser.convertToSingleRows(matchingPoints, queryParametrization.getTags());

		LOGGER.info(" Length: {}", matchingPoints.length);
		SQLContext sqlContext = new SQLContext(sparkContext);
		return executeSQLQuery(sparkContext.parallelize(rows), "SELECT * FROM rows", sqlContext, queryParametrization.getMetric(),generateTagsListFromMap(queryParametrization.getTags()));
	}
	
	private List<String> generateTagsListFromMap(Map<String,String> map){
		Object [] tagsArray =  map.keySet().toArray();
		List<String> tagsList = new ArrayList<String>();
		
		for(Object tag : tagsArray)
			tagsList.add(tag.toString());
		
		return tagsList;
	}

}