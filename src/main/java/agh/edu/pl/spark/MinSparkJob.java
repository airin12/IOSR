package agh.edu.pl.spark;

import net.opentsdb.core.TSDB;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.Comparator;

public class MinSparkJob extends AbstractSparkJob{

    public MinSparkJob(final TSDB tsdb){
        super(tsdb);
    }

    @Override
    protected Double execute(JavaRDD<Double> rdd){
        return rdd.min(new DoubleComparator());
    }

    private static class DoubleComparator implements Comparator<Double>, Serializable {
        static final long serialVersionUID = 1L;

        public int compare(Double first, Double second) {
            return Double.compare(first, second);
        }
    }
}
