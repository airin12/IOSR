package pl.edu.agh.spark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import pl.edu.agh.util.ConfigurationProvider;

public class SparkContextFactory {

	private static JavaSparkContext SPARK_CONTEXT = null;

	public JavaSparkContext getSparkContext() throws IOException {
		if (SPARK_CONTEXT == null) {
			ConfigurationProvider configProvider;

			configProvider = new ConfigurationProvider(ConfigurationProvider.CONFIGURATION_FILENAME);
			SparkConf conf = new SparkConf().setAppName(configProvider.getProperty(ConfigurationProvider.SPARK_APP_NAME_PROPERTY_NAME))
					.setMaster(configProvider.getProperty(ConfigurationProvider.SPARK_MASTER_URL_PROPERTY_NAME))
					.setJars(new String[] { configProvider.getProperty(ConfigurationProvider.SPARK_JAR_FILE_PROPERTY_NAME) });

			JavaSparkContext sparkContext = new JavaSparkContext(conf);
			SPARK_CONTEXT = sparkContext;
			return sparkContext;
		} else {
			return SPARK_CONTEXT;
		}
	}
	
	public void closeSparkContext(){
		if(SPARK_CONTEXT != null){
			SPARK_CONTEXT.stop();
			SPARK_CONTEXT.close();
			SPARK_CONTEXT = null;
		}
	}

}
