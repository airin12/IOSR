package agh.edu.pl.spark.job;

import net.opentsdb.core.TSDB;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class SumSparkJob extends AbstractSparkJob{

    public SumSparkJob(final TSDB tsdb, final JavaSparkContext sparkContext){
        super(tsdb, sparkContext);
    }

    @Override
    protected Object execute(JavaRDD<Double> rdd) {
        Double sum = rdd.reduce(new Function2<Double, Double, Double>() {
            private static final long serialVersionUID = 496017298043136095L;

            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        });
        return sum;
    }
}
