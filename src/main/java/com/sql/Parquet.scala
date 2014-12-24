

package com.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.reflect.runtime.universe

object Paquet extends App {

  val conf = new SparkConf().setAppName("hackaton").setMaster("local[4]").set("spark.executor.memory", "13g")
  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext._

  // Define the schema using a case class.
  case class Person(name: String, age: Int)

  // Create an RDD of Person objects and register it as a table.

  val people = sc.textFile("examples/src/main/resources/people.txt").
    map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
  people.registerAsTable("people")

  // The RDD is implicitly converted to a SchemaRDD  ## allowing it to be stored using parquet.
  people.saveAsParquetFile("people.parquet")

  // Read in the parquet file created above. Parquet files are 
  // self-describing so the schema is preserved.

  // The result of loading a parquet file is also a JavaSchemaRDD.
  val parquetFile = sqlContext.parquetFile("people.parquet")

  //Parquet files can also be registered as tables and then used in
  // SQL statements.

  parquetFile.registerAsTable("parquetFile")

  val teenagers =

    sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
  teenagers.collect().foreach(println)

}