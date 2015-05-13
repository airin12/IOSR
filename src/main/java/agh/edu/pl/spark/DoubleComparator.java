package agh.edu.pl.spark;

import java.io.Serializable;
import java.util.Comparator;


public class DoubleComparator implements Comparator<Double>, Serializable {

	public int compare(Double first, Double second) {
        return Double.compare(first, second);
    }
}
