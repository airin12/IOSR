package agh.edu.pl.spark;

import net.opentsdb.core.TSDB;

import org.apache.spark.api.java.JavaRDD;

public class MinSparkJob extends AbstractSparkJob{

    public MinSparkJob(final TSDB tsdb){
        super(tsdb);
    }

    @Override
    protected Double execute(JavaRDD<Double> rdd){
        return rdd.min(new DoubleComparator());
    }

}
