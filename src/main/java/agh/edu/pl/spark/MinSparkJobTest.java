package agh.edu.pl.spark;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.core.Aggregators;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

public class MinSparkJobTest {
    public static void main(String[] args) throws IOException {
        //TODO: initialize Config with path retrieved from configuration file
        //Config config = new Config("/path/to/opentsdb.conf");
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
                .setAggregator(Aggregators.SUM).build();
        Object result = new MinSparkJob(tsdb).execute(queryParametrization);
        System.out.print("Result=" + result);
    }
}
