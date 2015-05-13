package agh.edu.pl.spark;

import java.io.IOException;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import agh.edu.pl.rest.GrafanaService;
import agh.edu.pl.util.ConfigurationProvider;

public class SparkJobRunner {
	private static final String CONFIGURATION_FILENAME = "config.properties";
	private static final String TSDB_CONFIG_FILENAME_PROPERTY_NAME = "tsdb.config.file";

	public static void main( String[] args )
    {
		Config config;
		ConfigurationProvider configProvider;
		try {
			configProvider = new ConfigurationProvider(CONFIGURATION_FILENAME);
			config = new Config(configProvider.getProperty(TSDB_CONFIG_FILENAME_PROPERTY_NAME));
			//Thread-safe implementation of the TSDB client - we can use one instance per whole service
	        TSDB tsdb = new TSDB(config);
	        TSDBQueryParametrization queryParametrization = null;
	        try{
	        	queryParametrization = new TSDBQueryParametrizationBuilder().buildFromCombinedQuery(args[5]);
	        } catch( Exception ex){
	        	GrafanaService.resultMap.put("job","Error while creating TSDB query. Msg: "+ex.getMessage());
	        	return;
	        }
	        
	        Object result = new MinSparkJob(tsdb, configProvider).execute(queryParametrization);
	        GrafanaService.resultMap.put("job", result);
	        
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
