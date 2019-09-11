package com.myApplication.service.impl;

import com.myApplication.service.AvroConversion;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.validation.constraints.NotNull;

public class AvroConversionImpl implements AvroConversion {

	public void convertToAvro(String inputPath, String outputPath, String fileFormat) {

        SparkConf conf = new SparkConf().setAppName("AvroConversion").setMaster("local");

        try(JavaSparkContext jsc = new JavaSparkContext(conf);
            SparkSession sparkSession = new SparkSession(jsc.sc())) {

            String readFormat = null;
            if(fileFormat.equalsIgnoreCase("txt"))
                readFormat = "text";

            Dataset<Row> inputDataset = sparkSession.read()
                    .option("header", "parquet".equalsIgnoreCase(fileFormat) ? false : true)
                    .format(fileFormat.equalsIgnoreCase("txt") ? readFormat : fileFormat)
                    .load(inputPath);

            inputDataset.write().option("header", false).format("avro").save(outputPath);

        }
	}
}
