package pl.edu.agh.iosr.client;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;


public class SparkTsdbConnectorDemo {

	public static void main(String[] args) throws IOException{
		//true - tries to look for configuration in default location
		Config config = new Config(true);
		TSDB tsdb = new TSDB(config);
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("cpu","0");
		Query query= tsdb.newQuery();
		query.setStartTime(new Date(115, 3, 22).getTime());
		query.setEndTime(new Date().getTime());
		query.setTimeSeries("sys.cpu.nice", tags, Aggregators.SUM, false);
		DataPoints[] datapoints = query.run();
		DataPoints points = datapoints[0];
		System.out.println("Datapoints[].length=" + datapoints.length + " points.size()=" + points.size());
		List<Double> values = new LinkedList<Double>();
		for (int i =0; i < points.size(); i++){
			if (!points.isInteger(i)){
				values.add(points.doubleValue(i));
			}
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("testAppName").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		
		JavaRDD<Double> rddValues = sparkContext.parallelize(values);
		Double reducedValue = rddValues.reduce(new Function2<Double, Double, Double>() {
			public Double call(Double a, Double b) throws Exception {
				return a + b;
			}
		});
		System.out.println("reducedValue=" + reducedValue);
	}
}
