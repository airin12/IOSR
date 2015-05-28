package pl.edu.agh.spark.job;

import net.opentsdb.core.TSDB;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import pl.edu.agh.spark.DoubleComparator;

public class MinSparkJob extends AbstractSparkJob {

    public MinSparkJob(final TSDB tsdb, final JavaSparkContext sparkContext){
        super(tsdb, sparkContext);
    }

    @Override
    protected Double execute(JavaRDD<Double> rdd){
        return rdd.min(new DoubleComparator());
    }

}
