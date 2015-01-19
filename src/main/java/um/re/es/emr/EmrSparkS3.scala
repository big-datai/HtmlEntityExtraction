package um.re.es.emr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.MapWritable
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.apache.hadoop.io.Text
import org.apache.spark.serializer.KryoSerializer
import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter
import collection.JavaConversions._
import play.api.libs.json.Json
import play.api.libs.json.Writes
import play.api.libs.json._
import org.elasticsearch.spark.rdd.EsSpark
import um.re.es.emr.NumberFinder2
import org.elasticsearch.spark

object EmrSparkS3 {

  val conf_s = new SparkConf().setAppName("s3").set("master", "yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)
  val source = sc.textFile("s3://pavlovP/*.ready")
  val counts = source.flatMap { l => l.split(" ") }.map(word => (word, 1)).reduceByKey(_ + _)
  counts.count
  counts.saveAsTextFile("s3://pavlovout/15")

}