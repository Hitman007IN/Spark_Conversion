package com.myApplication.service.impl;

import com.myApplication.service.ParquetConversion;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetConversionImpl implements ParquetConversion {

	public void convertToParquet(String inputPath, String outputPath, String fileFormat) {

		SparkConf conf = new SparkConf().setAppName("ParquetConversion").setMaster("local");

		try(JavaSparkContext jsc = new JavaSparkContext(conf);
			SparkSession sparkSession = new SparkSession(jsc.sc())) {

			Dataset<Row> inputDataset = sparkSession.read()
										.option("header", fileFormat.equalsIgnoreCase("avro") ? false : true)
										.format(fileFormat.equalsIgnoreCase("txt") ? "text" : fileFormat)
										.load(inputPath);

			inputDataset.write().option("header", false).format("parquet").save(outputPath);

		}


	}
}
