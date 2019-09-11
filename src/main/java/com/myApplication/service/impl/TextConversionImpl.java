package com.myApplication.service.impl;

import com.myApplication.service.TextConversion;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TextConversionImpl implements TextConversion {

	public void convertToText(String inputPath, String outputPath, String fileFormat, String delimiter) {

		SparkConf conf = new SparkConf().setAppName("AvroConversion").setMaster("local");

		try(JavaSparkContext jsc = new JavaSparkContext(conf);
			SparkSession sparkSession = new SparkSession(jsc.sc())) {

			Dataset<Row> inputDataset = sparkSession.read()
					.option("header", "csv".equalsIgnoreCase(fileFormat) ? true : false)
					.format(fileFormat)
					.load(inputPath);

			inputDataset.createOrReplaceTempView("input");
			Dataset<Row> finalDataset = null;

			if(null != delimiter)
				finalDataset = sparkSession.sql("select concat_ws( '"+ delimiter +"', *) as oneCol from input");
			else
				finalDataset = sparkSession.sql("select concat(*) as oneCol from input");

			finalDataset.write().option("header", true).format("text").save(outputPath);

		}
	}
}
