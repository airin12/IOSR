package agh.edu.pl.spark;

import java.io.IOException;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import agh.edu.pl.rest.GrafanaService;

public class SparkJobRunner {
	public static void main( String[] args )
    {
		Config config;
		try {
			config = new Config("/root/files/opentsdb-2.0.1/src/opentsdb.conf");
			//Thread-safe implementation of the TSDB client - we can use one instance per whole service
	        TSDB tsdb = new TSDB(config);
//	        Map<String, String> tags = new HashMap<String, String>();
//	        tags.put("cpu","00");
//	        TSDBQueryParametrization queryParametrization = new TSDBQueryParametrizationBuilder()
//	                .setStartTime(new Date(115, 3, 24).getTime())
//	                .setEndTime(new Date().getTime())
//	                .setTags(tags)
//	                .setMetric("mem.usage.perc").build();
	        TSDBQueryParametrization queryParametrization = null;
	        try{
	        	queryParametrization = new TSDBQueryParametrizationBuilder().buildFromCombinedQuery(args[5]);
	        } catch( Exception ex){
	        	GrafanaService.resultMap.put("job","Error while creating TSDB query. Msg: "+ex.getMessage());
	        	return;
	        }
	        Object result = new MinSparkJob(tsdb).execute(queryParametrization);
	        GrafanaService.resultMap.put("job", result);
	        
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
