package pl.edu.agh.spark.job.test;

import java.io.IOException;
import java.util.List;

import net.opentsdb.core.TSDB;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;

import pl.edu.agh.spark.SparkSQLRDDExecutor;
import pl.edu.agh.spark.TSDBQueryParametrization;
import pl.edu.agh.spark.job.AbstractSparkJob;
import pl.edu.agh.util.ConfigurationProvider;
import scala.Tuple2;

public class LoadDataSparkJob extends AbstractSparkJob{

	public LoadDataSparkJob(TSDB tsdb, JavaSparkContext sparkContext) {
		super(tsdb, sparkContext);
	}

	@Override
	protected Object execute(JavaRDD<Double> rdd) {
		return null;
	}
	
	@Override
	public Object execute(TSDBQueryParametrization queryParametrization) {

		ConfigurationProvider configProvider;
		int numSlices = 1;

		try {
			configProvider = new ConfigurationProvider(ConfigurationProvider.CONFIGURATION_FILENAME);
			numSlices = Integer.parseInt(configProvider.getProperty(ConfigurationProvider.SPARK_SLAVES_NUMBER_PROPERTY_NAME));
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<Tuple2<Long, Long>> timestamps = generateTimestampsList(queryParametrization.getStartTime(), queryParametrization.getEndTime(), numSlices);

		
		SparkSQLRDDExecutor executor = new SparkSQLRDDExecutor();
		
		long start = System.nanoTime();
		
		JavaRDD<Row> rowRDD = executor.loadTSDBData(sparkContext.parallelizePairs(timestamps, numSlices), queryParametrization.toCombinedQuery());

		long end = System.nanoTime();
		
		List<Row> valuesInList = rowRDD.collect();
		
		
		return new Long(end-start);
	}

}
