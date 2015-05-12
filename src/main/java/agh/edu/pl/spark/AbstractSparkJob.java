package agh.edu.pl.spark;

import net.opentsdb.core.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public abstract class AbstractSparkJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MinSparkJob.class);
    private final TSDB tsdb;

    public AbstractSparkJob(final TSDB tsdb){
        this.tsdb = tsdb;
    }

    public Double execute(TSDBQueryParametrization queryParametrization){
        Query query = buildQuery(queryParametrization);
        DataPoints[] matchingPoints = query.run();
        if (matchingPoints.length == 0){
            LOGGER.info("No matching points for query found. Returning 0.0");
            return 0.0;
        }
        LOGGER.info("Fetched {} points.", matchingPoints[0].aggregatedSize());
        List<Double> values = extractValues(matchingPoints[0]);
        //TODO: read appName and master from configuration file
        SparkConf conf = new SparkConf().setAppName("example").setMaster("local");
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

    protected abstract Double execute(JavaRDD<Double> rdd);
}
