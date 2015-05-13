package agh.edu.pl.spark;


import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.core.Aggregators;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import agh.edu.pl.util.ConfigurationProvider;

public class MinSparkJobTest {
    private static final String CONFIGURATION_FILENAME = "config.properties";

    public static void main(String[] args) throws IOException {
        Config config = new Config(true);
        //Thread-safe implementation of the TSDB client - we can use one instance per whole service
        TSDB tsdb = new TSDB(config);
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("cpu","0");
        TSDBQueryParametrization queryParametrization = new TSDBQueryParametrizationBuilder()
                .setStartTime(new Date(115, 4, 24).getTime())
                .setEndTime(new Date().getTime())
                .setTags(tags)
                .setMetric("sys.cpu.nice")
                .setAggregator(Aggregators.SUM)
                .setMetric("sys.cpu.nice").build();
                
        ConfigurationProvider configProvider = new ConfigurationProvider(CONFIGURATION_FILENAME);
        Object result = new MinSparkJob(tsdb, configProvider).execute(queryParametrization);
        System.out.print("Result=" + result);
    }
}
