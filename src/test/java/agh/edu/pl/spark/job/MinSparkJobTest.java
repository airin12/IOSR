package agh.edu.pl.spark.job;


import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import agh.edu.pl.spark.TSDBQueryParametrization;
import agh.edu.pl.spark.TSDBQueryParametrizationBuilder;
import agh.edu.pl.spark.job.MinSparkJob;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import agh.edu.pl.util.ConfigurationProvider;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

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
        SparkConf conf = new SparkConf()
                .setAppName(configProvider.getProperty(ConfigurationProvider.SPARK_APP_NAME_PROPERTY_NAME))
                .setMaster(configProvider.getProperty(ConfigurationProvider.SPARK_MASTER_URL_PROPERTY_NAME))
                .setJars(new String[]{configProvider.getProperty(ConfigurationProvider.SPARK_JAR_FILE_PROPERTY_NAME)});

        // According to Apache Spark JavaDoc for JavaSparkContext:
        // "Only one SparkContext may be active per JVM. You must stop() the active SparkContext
        // before creating a new one."
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        Object result = new MinSparkJob(tsdb, sparkContext).execute(queryParametrization);
        System.out.print("Result=" + result);
    }
}
