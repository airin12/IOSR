package agh.edu.pl.spark;

import java.io.IOException;

import agh.edu.pl.spark.job.MinSparkJob;
import agh.edu.pl.spark.job.SqlSparkJob;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import agh.edu.pl.rest.GrafanaService;
import agh.edu.pl.util.ConfigurationProvider;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkJobRunner {

private static final Logger LOGGER = LogManager.getLogger(SparkJobRunner.class);
	
	public static void main( String[] args )
    {
		Config config;
		ConfigurationProvider configProvider;
		String mode = args[5];
		LOGGER.debug(args[5]);
		LOGGER.debug(args[6]);
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

	        TSDBQueryParametrization queryParametrization = null;
	        try{
	        	if(mode.equals("function"))
	        		queryParametrization = new TSDBQueryParametrizationBuilder().buildFromCombinedQuery(args[6]);
	        	else if(mode.equals("json"))
	        		queryParametrization = new TSDBQueryParametrizationBuilder().buildFromJson(args[6]);

	        } catch( Exception ex){
	        	GrafanaService.resultMap.put("job","Error while creating TSDB query. Msg: "+ex.getMessage());
	        	return;
	        }
	        
	        Object result = null;
	        if(mode.equals("function"))
	        	result = new MinSparkJob(tsdb, sparkContext).execute(queryParametrization);
	        else if(mode.equals("json"))
	        	result = new SqlSparkJob(tsdb, sparkContext).execute(queryParametrization);
	        
	        GrafanaService.resultMap.put("job", result);
	        
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
