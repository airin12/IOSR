package agh.edu.pl.spark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import agh.edu.pl.util.ConfigurationProvider;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSparkJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MinSparkJob.class);
    private static final String SPARK_MASTER_URL_PROPERTY_NAME = "spark.master.url";
    private static final String SPARK_APP_NAME_PROPERTY_NAME = "spark.app.name";
    private static final String SPARK_JAR_FILE_PROPERTY_NAME = "spark.jar.file";
    private final TSDB tsdb;
    private final ConfigurationProvider configProvider;

    public AbstractSparkJob(final TSDB tsdb, final ConfigurationProvider configProvider){
        this.tsdb = tsdb;
        this.configProvider = configProvider;
    }

    public Object execute(TSDBQueryParametrization queryParametrization){
        Query query = buildQuery(queryParametrization);
        DataPoints[] matchingPoints = query.run();
        if (matchingPoints.length == 0){
            LOGGER.info("No matching points for query found. Returning 0.0");
            return 0.0;
        }
        LOGGER.info("Fetched {} points.", matchingPoints[0].aggregatedSize());
        List<Double> values = extractValues(matchingPoints[0]);
        SparkConf conf = new SparkConf()
                .setAppName(configProvider.getProperty(SPARK_APP_NAME_PROPERTY_NAME))
                .setMaster(configProvider.getProperty(SPARK_MASTER_URL_PROPERTY_NAME))
                .setJars(new String[]{configProvider.getProperty(SPARK_JAR_FILE_PROPERTY_NAME)});
        JavaSparkContext context = new JavaSparkContext(conf);
        try {
            return execute(context.parallelize(values));
        } finally {
            context.close();
        }
    }

    private Query buildQuery(TSDBQueryParametrization queryParametrization) {
        Query query= tsdb.newQuery();
        query.setStartTime(queryParametrization.getStartTime());
        query.setEndTime(queryParametrization.getEndTime());
        query.setTimeSeries(queryParametrization.getMetric(), queryParametrization.getTags(), Aggregators.SUM, false);
        return query;
    }

    private List<Double> extractValues(DataPoints matchingPoint) {
        SeekableView iterator = matchingPoint.iterator();
        List<Double> values = new LinkedList<Double>();
        while(iterator.hasNext()){
            values.add(iterator.next().toDouble());
        }
        return values;
    }

    protected abstract Object execute(JavaRDD<Double> rdd);
}
