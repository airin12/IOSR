package agh.edu.pl.spark;

import java.io.IOException;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import agh.edu.pl.rest.GrafanaService;
import agh.edu.pl.spark.job.MinSparkJob;
import agh.edu.pl.spark.job.SparkJob;
import agh.edu.pl.spark.job.SqlSparkJob;
import agh.edu.pl.spark.job.SumSparkJob;
import agh.edu.pl.util.ConfigurationProvider;

public class SparkJobRunner {

private static final Logger LOGGER = LogManager.getLogger(SparkJobRunner.class);
	
	public static void main( String[] args )
    {
		Config config;
		ConfigurationProvider configProvider;

		LOGGER.info(" SparkJobRunner starting in mode: {}",args[0]);
		SparkJobRunnerModes mode = SparkJobRunnerModes.valueOf(args[0]);
		
		try {
			configProvider = new ConfigurationProvider(ConfigurationProvider.CONFIGURATION_FILENAME);
			config = new Config(configProvider.getProperty(ConfigurationProvider.TSDB_CONFIG_FILENAME_PROPERTY_NAME));
			//Thread-safe implementation of the TSDB client - we can use one instance per whole service
	        TSDB tsdb = new TSDB(config);
			SparkConf conf = new SparkConf()
					.setAppName(configProvider.getProperty(ConfigurationProvider.SPARK_APP_NAME_PROPERTY_NAME))
					.setMaster(configProvider.getProperty(ConfigurationProvider.SPARK_MASTER_URL_PROPERTY_NAME))
					.setJars(new String[]{configProvider.getProperty(ConfigurationProvider.SPARK_JAR_FILE_PROPERTY_NAME)});
	
			// According to Apache Spark JavaDoc for JavaSparkContext:
			// "Only one SparkContext may be active per JVM. You must stop() the active SparkContext
			// before creating a new one."
			JavaSparkContext sparkContext = new JavaSparkContext(conf);
			try {
				TSDBQueryParametrization queryParametrization = null;
				try {
					if (mode.equals(SparkJobRunnerModes.BASIC))
						queryParametrization = new TSDBQueryParametrizationBuilder().buildFromCombinedQuery(args[1]);
					else if (mode.equals(SparkJobRunnerModes.SQL))
						queryParametrization = new TSDBQueryParametrizationBuilder().buildFromJson(args[1].replace(";", " "));

				} catch (Exception ex) {
					GrafanaService.resultMap.put("job", "Error while creating TSDB query. Msg: " + ex.getMessage());
					LOGGER.error("Exception while creating TSDB query",ex);
					sparkContext.close();
					return;
				}

				Object result = null;
				if (mode.equals(SparkJobRunnerModes.BASIC)) {
					SparkJob job = getSparkJob(queryParametrization.getAggregator(), tsdb, sparkContext);
					result = job.execute(queryParametrization);
				} else if (mode.equals(SparkJobRunnerModes.SQL)) {
					result = new SqlSparkJob(tsdb, sparkContext).execute(queryParametrization);
				} 
				LOGGER.debug(" Result of operation is: {}",result);
				GrafanaService.resultMap.put("job", result);
			} finally {
				if (sparkContext != null){
					sparkContext.stop();
					sparkContext.close();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
    }

	private static SparkJob getSparkJob(Aggregator aggregator, TSDB tsdb, JavaSparkContext sparkContext) {
		if (Aggregators.SUM.equals(aggregator)){
			return new SumSparkJob(tsdb, sparkContext);
		} else {
			return new MinSparkJob(tsdb, sparkContext);
		}
	}
}
