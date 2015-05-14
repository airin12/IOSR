package agh.edu.pl.spark;

import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;

public class DoubleSumator implements Function2<Double, Double, Double>, Serializable {
    private static final long serialVersionUID = 496017298043136095L;

    @Override
    public Double call(Double v1, Double v2) throws Exception {
        return v1 + v2;
    }
}
