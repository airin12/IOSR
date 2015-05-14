package agh.edu.pl.spark.job;

import agh.edu.pl.spark.DoubleComparator;
import agh.edu.pl.spark.job.AbstractSparkJob;
import agh.edu.pl.util.ConfigurationProvider;
import net.opentsdb.core.TSDB;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MinSparkJob extends AbstractSparkJob {

    public MinSparkJob(final TSDB tsdb, final JavaSparkContext sparkContext){
        super(tsdb, sparkContext);
    }

    @Override
    protected Double execute(JavaRDD<Double> rdd){
        return rdd.min(new DoubleComparator());
    }

}
