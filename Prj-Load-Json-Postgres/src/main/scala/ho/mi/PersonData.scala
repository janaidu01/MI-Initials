package ho.mi

/* This program reads data from CSV file into a DataFrame and using the DataFrame creates a table in Postgres Database */
/* Observation : - Though jars were available in SPARK_HOME/jars library dependencies needed to be added to build.sbt to package and execute the Program */


import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Personidata {
  def main(args: Array[String]) {

  val spark = SparkSession
      .builder
      .master("local")	
      .appName("PersonData")
      .getOrCreate()
	  
	// Registering Driver - Might be redundant in some cases  
	Class.forName("org.postgresql.Driver").newInstance

	//Set database connection properties
	val dbProperties = new java.util.Properties
	dbProperties.load(new java.io.FileInputStream(new java.io.File("db-properties.flat")));
	val jdbcUrl = dbProperties.getProperty("jdbcUrl")

	
	//val df_id=spark.read.format("json").option("header", "true").load("person.json")
	val df_person=spark.read.json("person.json")
	
	//Flatten the array in JSON and load into "Identity" table
	val df_id_handle=df_person.select(explode(df_person("identity_handles"))).toDF("identity_handles")
	val df_id_handle_flatten=df_id_handle.select("identity_handles.unique_identifier","identity_handles.visibility_marker","identity_handles.interface_identifier")

	//Put all elements in JSON, except the "identity_handles" into "Person" table
	val df_person_data=df_person.select("internal_handle.visibility_marker", "internal_handle.interface_identifier", "person_space", "created", "created_by")
	
	
	// Creating table 'Person' in the Postgres database	
	var tab_name1="person_details"
	df_person_data.write.mode("error").jdbc(jdbcUrl, tab_name1, dbProperties)

	// Creating table 'Identity' in the Postgres database	
	var tab_name2="identity"
	df_id_handle_flatten.write.mode("overwrite").jdbc(jdbcUrl, tab_name2, dbProperties)


   spark.stop()
  }
}	  



