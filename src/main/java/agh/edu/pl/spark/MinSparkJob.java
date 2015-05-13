package agh.edu.pl.spark;

import agh.edu.pl.util.ConfigurationProvider;
import net.opentsdb.core.TSDB;

import org.apache.spark.api.java.JavaRDD;

public class MinSparkJob extends AbstractSparkJob{

    public MinSparkJob(final TSDB tsdb, final ConfigurationProvider configProvider){
        super(tsdb, configProvider);
    }

    @Override
    protected Double execute(JavaRDD<Double> rdd){
        return rdd.min(new DoubleComparator());
    }

}
