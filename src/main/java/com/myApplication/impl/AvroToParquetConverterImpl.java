package com.myApplication.impl;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AvroToParquetConverterImpl {

	public void convertToParquet(String inputPath, String outputPath, String delimiter, JavaSparkContext jsc) {
		
		final SparkSession sparkSession = new SparkSession(jsc.sc());
		try {
			Dataset<Row> par = sparkSession.read().format("com.databricks.spark.avro").load(inputPath);
			
			final File outputFilePath = new File(outputPath);
			if (outputFilePath.exists()) {

				try {
					FileUtils.deleteDirectory(outputFilePath);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			par.write().parquet(outputPath);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			sparkSession.close();
		}
	}

}
