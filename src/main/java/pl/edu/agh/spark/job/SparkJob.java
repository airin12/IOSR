package pl.edu.agh.spark.job;

import pl.edu.agh.spark.TSDBQueryParametrization;

public interface SparkJob {
    Object execute(TSDBQueryParametrization queryParametrization);
}
