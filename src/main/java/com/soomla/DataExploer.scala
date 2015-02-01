package com.soomla
import java.io.File
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import um.re.es.emr.URegistrator
import um.re.es.emr.PriceParcer
import scala.math
import scala.collection.JavaConversions._
import play.api.libs.json._
import play.api.libs.json.{ Json, JsValue, JsObject, JsArray }
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.elasticsearch.spark
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.SparkContext
import org.apache.hadoop.io.NullWritable
import org.elasticsearch.hadoop.mr.EsOutputFormat
import org.apache.hadoop.io.compress.CompressionCodecFactory
object DataExploer extends App{

   val conf_s = new SparkConf().setAppName("soomla").set("master", "spark://Dmitrys-MacBook-Pro.local:7077")//.set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)
  
   val source = sc.textFile("file:///Users/dmitry/soomla/events.json")
  val counts = source.flatMap { l => l.split(" ") }.map(word => (word, 1)).reduceByKey(_ + _)
  val source4 = sc.textFile("s3://touchbeam-datascience/archivefile3.zip")

}