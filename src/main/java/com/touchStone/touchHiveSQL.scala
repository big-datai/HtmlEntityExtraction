package com.touchStone
/*
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

object touchHiveSQL {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("touchstone").setMaster("local[20]").set("spark.executor.memory", "50g")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    // sc is an existing SparkContext.

    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    sqlContext.sql("LOAD DATA LOCAL INPATH '/user/boris/DetectionQuery1_data/000000_0' INTO TABLE src")

    // Queries are expressed in HiveQL
    sqlContext.sql("FROM src SELECT key, value").collect().foreach(println)
  }

}
*/