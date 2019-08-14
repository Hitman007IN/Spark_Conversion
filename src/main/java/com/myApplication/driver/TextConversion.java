package com.myApplication.driver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.myApplication.impl.ParquetToTextConverterImpl;

public class TextConversion {

	public static void main(String[] args) {
		
		final String inputPath = args[0];
		final String outputPath = args[1];
		
		final SparkConf conf = new SparkConf().setAppName("CsvConversion").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		if(args == null || args.length < 2 ) {
			jsc.close();
			throw new ArrayIndexOutOfBoundsException();
		}
		
		ParquetToTextConverterImpl parquetToTextConverterImpl = new ParquetToTextConverterImpl();
		parquetToTextConverterImpl.convertText(inputPath, outputPath, jsc);
		
		jsc.close();
	}
}
