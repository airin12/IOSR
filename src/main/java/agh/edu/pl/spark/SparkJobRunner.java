package agh.edu.pl.spark;

import java.io.IOException;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import agh.edu.pl.rest.GrafanaService;
import agh.edu.pl.util.ConfigurationProvider;

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
	        	result = new MinSparkJob(tsdb, configProvider).execute(queryParametrization);
	        else if(mode.equals("json"))
	        	result = new SqlSparkJob(tsdb, configProvider).execute(queryParametrization);
	        
	        GrafanaService.resultMap.put("job", result);
	        
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
