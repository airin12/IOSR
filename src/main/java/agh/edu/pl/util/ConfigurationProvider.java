package agh.edu.pl.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationProvider {
    private final Properties props;

    public static final String SPARK_MASTER_URL_PROPERTY_NAME = "spark.master.url";
    public static final String SPARK_APP_NAME_PROPERTY_NAME = "spark.app.name";
    public static final String SPARK_JAR_FILE_PROPERTY_NAME = "spark.jar.file";
    public static final String CONFIGURATION_FILENAME = "config.properties";
    public static final String TSDB_CONFIG_FILENAME_PROPERTY_NAME = "tsdb.config.file";
    public static final String TSDB_SLAVE_CONFIG_FILENAME_PROPERTY_NAME = "tsdb.slave.config.file";
    public static final String SPARK_SLAVES_NUMBER_PROPERTY_NAME = "spark.slaves.number";
    
    public ConfigurationProvider(String configFilename) throws IOException{
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        props = new Properties();
        InputStream resourceStream = null;
        try{
            resourceStream = classLoader.getResourceAsStream(configFilename);
            props.load(resourceStream);
        } finally {
            if (resourceStream != null) {
                resourceStream.close();
            }
        }
    }

    public String getProperty(String propertyName){
        return props.getProperty(propertyName);
    }
}
