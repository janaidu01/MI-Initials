package ho.mi

/* This program reads data from CSV file into a DataFrame and using the DataFrame creates a table in Postgres Database */
/* Observation : - Though jars were available in SPARK_HOME/jars library dependencies needed to be added to build.sbt to package and execute the Program */


import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

object Jdbc_Csv {
  def main(args: Array[String]) {

  val spark = SparkSession
      .builder
      .master("local")	
      .appName("JDBC CSV")
      .getOrCreate()
	  
	// Registering Driver - Might be redundant in some cases  
	Class.forName("org.postgresql.Driver").newInstance

	// Reading from csv file
	val df=spark.read.format("csv").option("header", "false").load("person.csv")
	
	// Setting database connection details
	val dbProperties = new java.util.Properties
	dbProperties.load(new java.io.FileInputStream(new java.io.File("db-properties.flat")));
	val jdbcUrl = dbProperties.getProperty("jdbcUrl")

	// Creating table 'PersonData' in the Postgres database	
	var where="PersonData"
	df.write.mode("error").jdbc(jdbcUrl, where, dbProperties)
	
   spark.stop()
  }
}	  
