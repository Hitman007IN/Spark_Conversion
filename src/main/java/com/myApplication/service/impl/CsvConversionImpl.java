package com.myApplication.service.impl;

import com.myApplication.service.CsvConversion;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.validation.constraints.NotNull;

public class CsvConversionImpl implements CsvConversion {

	public void convertToCsv(String inputPath, String outputPath, String fileFormat) {
		
		SparkConf conf = new SparkConf().setAppName("CsvConversion").setMaster("local");

		try(JavaSparkContext jsc = new JavaSparkContext(conf);
			SparkSession sparkSession = new SparkSession(jsc.sc())) {

			String readFormat = null;
			if(fileFormat.equalsIgnoreCase("txt"))
				readFormat = "text";

			Dataset<Row> inputDataset = sparkSession.read()
										.option("header", "text".equalsIgnoreCase(readFormat) ? true : false)
										.format(fileFormat.equalsIgnoreCase("txt") ? readFormat : fileFormat)
										.load(inputPath);

			inputDataset.write().option("header", true).format("csv").save(outputPath);

		}
	}
}
