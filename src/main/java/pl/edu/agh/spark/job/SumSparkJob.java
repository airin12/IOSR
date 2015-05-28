package pl.edu.agh.spark.job;

import net.opentsdb.core.TSDB;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import pl.edu.agh.spark.DoubleSumator;

public class SumSparkJob extends AbstractSparkJob{

    public SumSparkJob(final TSDB tsdb, final JavaSparkContext sparkContext){
        super(tsdb, sparkContext);
    }

    @Override
    protected Object execute(JavaRDD<Double> rdd) {
        return rdd.reduce(new DoubleSumator());
    }
}
