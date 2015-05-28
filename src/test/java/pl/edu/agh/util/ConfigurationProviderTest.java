package pl.edu.agh.util;

import org.junit.Before;
import org.junit.Test;

import pl.edu.agh.util.ConfigurationProvider;

import java.io.IOException;

import static org.junit.Assert.*;

public class ConfigurationProviderTest {
    private static final String CONFIGURATION_FILENAME = "config.properties";
    private static final String SPARK_MASTER_URL_PROPERTY_NAME = "spark.master.url";
    private static final String EXPECTED_SPARK_MASTER_URL_VALUE = "spark://172.17.84.76:7077";
    private static final String TSDB_CONFIG_FILENAME_PROPERTY_NAME = "tsdb.config.file";
    private static final String EXPECTED_TSDB_CONFIG_FILENAME = "/root/files/opentsdb-2.0.1/src/opentsdb.conf";
    private static final String SPARK_APP_NAME_PROPERTY_NAME = "spark.app.name";
    private static final String EXPECTED_SPARK_APP_NAME = "Grafana Service";
    private static final String SPARK_JAR_FILE_PROPERTY_NAME = "spark.jar.file";
    private static final String EXPECTED_SPARK_JAR_FILENAME = "/root/files/spark.jar";
    private ConfigurationProvider configurationProvider;

    @Before
    public void setUp() throws IOException {
        configurationProvider = new ConfigurationProvider(CONFIGURATION_FILENAME);
    }

    @Test
    public void shouldReadSparkMasterUrl(){
        assertEquals(EXPECTED_SPARK_MASTER_URL_VALUE, configurationProvider.getProperty(SPARK_MASTER_URL_PROPERTY_NAME));
    }

    @Test
    public void shouldReadTsdbConfigFilename(){
        assertEquals(EXPECTED_TSDB_CONFIG_FILENAME, configurationProvider.getProperty(TSDB_CONFIG_FILENAME_PROPERTY_NAME));
    }

    @Test
    public void shouldReadSparkAppName(){
        assertEquals(EXPECTED_SPARK_APP_NAME, configurationProvider.getProperty(SPARK_APP_NAME_PROPERTY_NAME));
    }

    @Test
    public void shouldReadSparkJarFile(){
        assertEquals(EXPECTED_SPARK_JAR_FILENAME, configurationProvider.getProperty(SPARK_JAR_FILE_PROPERTY_NAME));
    }
}