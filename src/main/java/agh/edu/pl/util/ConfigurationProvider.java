package agh.edu.pl.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationProvider {
    private final Properties props;

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
