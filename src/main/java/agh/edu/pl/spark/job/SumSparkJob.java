package agh.edu.pl.spark.job;

import agh.edu.pl.spark.DoubleSumator;
import net.opentsdb.core.TSDB;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SumSparkJob extends AbstractSparkJob{

    public SumSparkJob(final TSDB tsdb, final JavaSparkContext sparkContext){
        super(tsdb, sparkContext);
    }

    @Override
    protected Object execute(JavaRDD<Double> rdd) {
        return rdd.reduce(new DoubleSumator());
    }
}
