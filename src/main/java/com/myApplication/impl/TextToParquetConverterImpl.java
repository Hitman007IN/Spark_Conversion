package com.myApplication.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.myApplication.property.PropertyReader;

public class TextToParquetConverterImpl {

	public void convertToParquet(String inputPath, String outputPath, String delimiter, JavaSparkContext jsc) {
		
		final SparkSession sparkSession = new SparkSession(jsc.sc());
		try {
			JavaRDD<String> data = jsc.textFile(inputPath);
			JavaRDD<Row> rows = data.map(new Function<String, Row>() {
				private static final long serialVersionUID = -4332903997027358601L;

				@Override
				public Row call(String line) throws Exception {
					/*
					 * If row ends with delimiter, add space to prevent array index out of bounds.
					 * This is because spark assumes that there has been no data except the
					 * delimiter.
					 * 
					 */
					if (line.endsWith(delimiter)) {
						line = line + " ";
					}
					return RowFactory.create((Object[]) line.split(null != delimiter ? delimiter : ""));
				}
			});

			List<StructField> fields = new ArrayList<StructField>();

			PropertyReader propertyReader = new PropertyReader();
			String schemaString = propertyReader.getPropertyValue("schema");

			for (String fieldName : schemaString.split(",")) {
				fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
			}

			StructType schema = DataTypes.createStructType(fields);
			Dataset<Row> dataSet = sparkSession.createDataFrame(rows, schema);
			
			final File outputFilePath = new File(outputPath);
			if (outputFilePath.exists()) {

				try {
					FileUtils.deleteDirectory(outputFilePath);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			dataSet.write().parquet(outputPath);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			sparkSession.close();
		}
		
	}

}
