package io.teknek.farsandra.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class ConfigHolder {

	private static Logger LOGGER = Logger.getLogger(ConfigHolder.class);

	private final static String propertyFileName = "farsandra.properties";

	private Properties properties;

	public ConfigHolder() {
		properties = new Properties();
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propertyFileName);

		if (inputStream != null) {
			try {
				properties.load(inputStream);
			} catch (IOException e) {
				LOGGER.error("property file '" + propertyFileName + "' not found");
			}
		}
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}
}
