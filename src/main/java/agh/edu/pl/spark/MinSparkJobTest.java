package agh.edu.pl.spark;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
                .setStartTime(new Date(115, 3, 22).getTime())
                .setEndTime(new Date().getTime())
                .setTags(tags)
                .setMetric("sys.cpu.nice").build();
        Double result = new MinSparkJob(tsdb).execute(queryParametrization);
        System.out.print("Result=" + result);
    }
}
