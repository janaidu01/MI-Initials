package main.java.buri.sparkour;

import org.apache.spark.sql.SparkSession;
//import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**

 * Loads a DataFrame from a relational database table over JDBC,
 * manipulates the data, and saves the results back to a table.
 */

public final class JUsingJDBC {

	public static void main(String[] args) throws Exception {

		/*SparkConf sparkConf = new SparkConf().setAppName("JUsingJDBC");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sc);
         */
		
		SparkSession sparkSession = SparkSession
				   .builder()
			      .master("local")
			      .appName("spark jdbc program")
			      .getOrCreate();
			      
		      
		// Load properties from file
		Properties dbProperties = new Properties();
		dbProperties.load(new FileInputStream(new File("db-properties.flat")));
		String jdbcUrl = dbProperties.getProperty("jdbcUrl");

		System.out.println("A DataFrame loaded from the entire contents of a table over JDBC.");
		String where = "people";
		Dataset<Row> entireDF = sparkSession.read().jdbc(jdbcUrl, where, dbProperties);
		entireDF.printSchema();

		entireDF.show();

		System.out.println("Filtering the table to just show the males.");

		entireDF.filter("is_male = 1").show();

		System.out.println("Alternately, pre-filter the table for males before loading over JDBC.");

		where = "(select * from people where is_male = 1) as subset";

		Dataset<Row>  malesDF = sparkSession.read().jdbc(jdbcUrl, where, dbProperties);

		malesDF.show();

		System.out.println("Update weights by 2 pounds (results in a new DataFrame with same column names)");

		Dataset<Row>  heavyDF = entireDF.withColumn("updated_weight_lb", entireDF.col("weight_lb").plus(2));

		Dataset<Row> updatedDF = heavyDF.select("id", "name", "is_male", "height_in", "updated_weight_lb")

			.withColumnRenamed("updated_weight_lb", "weight_lb");

		updatedDF.show();

		System.out.println("Save the updated data to a new table with JDBC");

		where = "updated_people";

		updatedDF.write().mode("error").jdbc(jdbcUrl, where, dbProperties);

		System.out.println("Load the new table into a new DataFrame to confirm that it was saved successfully.");

		Dataset<Row>  retrievedDF = sparkSession.read().jdbc(jdbcUrl, where, dbProperties);

		retrievedDF.show();

		sparkSession.stop();

	}

}
