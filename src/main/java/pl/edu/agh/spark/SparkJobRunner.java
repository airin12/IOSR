package pl.edu.agh.spark;

import java.io.IOException;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import pl.edu.agh.rest.GrafanaService;
import pl.edu.agh.spark.job.MinSparkJob;
import pl.edu.agh.spark.job.SparkJob;
import pl.edu.agh.spark.job.SqlSparkJob;
import pl.edu.agh.spark.job.SumSparkJob;
import pl.edu.agh.spark.job.test.LoadDataSparkJob;
import pl.edu.agh.util.ConfigurationProvider;

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
	        TSDB tsdb = null;
	        if(mode.equals(SparkJobRunnerModes.BASIC) || mode.equals(SparkJobRunnerModes.TEST))
	        	tsdb = new TSDB(config);
			
			JavaSparkContext sparkContext = new SparkContextFactory().getSparkContext();
			
			try {
				TSDBQueryParametrization queryParametrization = null;
				try {
					if (mode.equals(SparkJobRunnerModes.BASIC) || mode.equals(SparkJobRunnerModes.TEST))
						queryParametrization = new TSDBQueryParametrizationBuilder().buildFromCombinedQuery(args[1]);
					
				} catch (Exception ex) {
					GrafanaService.resultMap.put("job", "Error while creating TSDB query. Msg: " + ex.getMessage());
					LOGGER.error("Exception while creating TSDB query",ex);
					return;
				}

				Object result = null;
				if (mode.equals(SparkJobRunnerModes.BASIC)) {
					SparkJob job = getSparkJob(queryParametrization.getAggregator(), tsdb, sparkContext);
					result = job.execute(queryParametrization);
				} else if (mode.equals(SparkJobRunnerModes.SQL)) {
					result = new SqlSparkJob(tsdb, sparkContext).executeJsonQuery(args[1]);
				} else if (mode.equals(SparkJobRunnerModes.TEST)){
					result = new LoadDataSparkJob(tsdb, sparkContext).execute(queryParametrization);
				}
				
				LOGGER.debug(" Result of operation is: {}",result);
				GrafanaService.resultMap.put("job", result);
			} catch (Exception ex){
				LOGGER.error("Exception while executing job",ex);
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
