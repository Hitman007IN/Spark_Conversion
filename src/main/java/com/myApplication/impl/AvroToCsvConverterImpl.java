package com.myApplication.impl;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AvroToCsvConverterImpl {

	public static void convertCsv(String inputPath, String outputPath, JavaSparkContext jsc) {
		
		SparkSession sparkSession = new SparkSession(jsc.sc());
		Dataset<Row> inputdataset = sparkSession.read().format("avro").load(inputPath);
		
		inputdataset.write().csv(outputPath);
		
		sparkSession.close();
	}
}
