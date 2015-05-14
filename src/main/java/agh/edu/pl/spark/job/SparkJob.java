package agh.edu.pl.spark.job;

import agh.edu.pl.spark.TSDBQueryParametrization;

public interface SparkJob {
    Object execute(TSDBQueryParametrization queryParametrization);
}
