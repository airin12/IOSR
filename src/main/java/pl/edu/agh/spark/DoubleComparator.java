package pl.edu.agh.spark;

import java.io.Serializable;
import java.util.Comparator;


public class DoubleComparator implements Comparator<Double>, Serializable {

	private static final long serialVersionUID = 1L;

	public int compare(Double first, Double second) {
        return Double.compare(first, second);
    }
}
