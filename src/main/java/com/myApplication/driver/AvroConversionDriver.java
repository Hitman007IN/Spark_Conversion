package com.myApplication.driver;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.myApplication.impl.ParquetToAvroConverterImpl;

public class AvroConversionDriver {

	public static void main(String[] args) {
		
		final String inputPath = args[0];
		final String outputPath = args[1];
		
		final SparkConf conf = new SparkConf().setAppName("CsvConversion").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		if(args == null || args.length < 2 ) {
			jsc.close();
			throw new ArrayIndexOutOfBoundsException();
		}
		
		ParquetToAvroConverterImpl ParquetToAvroConverterImpl = new ParquetToAvroConverterImpl();
		
		final File outputFilePath = new File(outputPath);
		if (outputFilePath.exists()) {

			try {
				FileUtils.deleteDirectory(outputFilePath);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		ParquetToAvroConverterImpl.convertToAvro(inputPath, outputPath, jsc);
		
		jsc.close();
	}
}
