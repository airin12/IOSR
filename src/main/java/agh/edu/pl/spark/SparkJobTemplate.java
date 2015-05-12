package agh.edu.pl.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import agh.edu.pl.rest.GrafanaService;

public class SparkJobTemplate {
	public static void main( String[] args )
    {
    	SparkConf conf = new SparkConf().setAppName("example").setMaster("spark://172.17.84.76:7077");
    	JavaSparkContext context = new JavaSparkContext(conf);
    	
    	JavaRDD<String> textFile = context.textFile("hdfs://172.17.84.76:9000/README.txt");
    	long result = textFile.count();	
    	GrafanaService.resultMap.put("job", new Long(result));
    	context.close();
    }
}
