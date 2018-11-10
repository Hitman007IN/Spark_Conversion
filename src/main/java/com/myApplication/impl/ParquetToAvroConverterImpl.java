package com.myApplication.impl;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetToAvroConverterImpl {

	public void convertToAvro(String inputPath, String outputPath, JavaSparkContext jsc) {
		
		SparkSession sparkSession = new SparkSession(jsc.sc());
		
		Dataset<Row> dataset = sparkSession.read().parquet(inputPath);
		
		final File outputFilePath = new File(outputPath);
		if (outputFilePath.exists()) {

			try {
				FileUtils.deleteDirectory(outputFilePath);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		dataset.write().format("com.databricks.spark.avro").save(outputPath);
		
		sparkSession.close();
	}
}
