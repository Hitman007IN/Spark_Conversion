package com.myApplication.driver;

import org.apache.commons.io.FilenameUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import com.myApplication.impl.AvroToParquetConverterImpl;
import com.myApplication.impl.TextToParquetConverterImpl;

public class ParquetConversionDriver {

	public static void main(String[] args) {
		
		final SparkConf conf = new SparkConf().setAppName("ParquetConversion").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		final String inputPath = args[0];
		final String outputPath = args[1];
		String delimiter = args[2];
		
		if(args == null || args.length != 3 ) {
			jsc.close();
			throw new ArrayIndexOutOfBoundsException();
		}
		
		if (null != delimiter && delimiter.contains("|")) {
			delimiter = "\\|";
		}
		
		TextToParquetConverterImpl textToParquetConverterImpl = null;
		AvroToParquetConverterImpl avroToParquetConverterImpl = null;
		
		String extension = FilenameUtils.getExtension(inputPath);
		if("txt".equalsIgnoreCase(extension) || "csv".equalsIgnoreCase(extension)) {
			textToParquetConverterImpl = new TextToParquetConverterImpl();
			textToParquetConverterImpl.convertToParquet(inputPath, outputPath, delimiter, jsc);
		}else if("avro".equalsIgnoreCase(extension)) {
			avroToParquetConverterImpl = new AvroToParquetConverterImpl();
			avroToParquetConverterImpl.convertToParquet(inputPath, outputPath, delimiter, jsc);
		}
		
		jsc.close();
	}
}
