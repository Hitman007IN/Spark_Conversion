package com.myApplication.property;

import java.io.InputStream;
import java.util.Properties;

public class PropertyReader {

	public static final String PROPERTY_FILE_LOCATION = "/parquetSchema.properties";
	
	private static Properties prop;

	static {

		prop = new Properties();
		try(InputStream inputStream = PropertyReader.class.getResourceAsStream(PROPERTY_FILE_LOCATION)){
			prop.load(inputStream);
		} catch (Exception exe) {
			System.out.println("*********error while loading property file************");
			exe.printStackTrace();
		}
	}

	public static String getPropertyValue(String key) {
		return prop.getProperty(key);
	}
}
