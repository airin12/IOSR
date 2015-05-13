package agh.edu.pl.spark;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import agh.edu.pl.rest.GrafanaService;
import agh.edu.pl.util.ConfigurationProvider;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

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
	        Map<String, String> tags = new HashMap<String, String>();
	        tags.put("cpu","00");
	        TSDBQueryParametrization queryParametrization = new TSDBQueryParametrizationBuilder()
	                .setStartTime(new Date(115, 3, 24).getTime())
	                .setEndTime(new Date().getTime())
	                .setTags(tags)
	                .setMetric("mem.usage.perc").build();
	        Object result = new MinSparkJob(tsdb, configProvider).execute(queryParametrization);
	        GrafanaService.resultMap.put("job", result);
	        
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
