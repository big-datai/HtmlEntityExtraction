package ginger
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
import um.re.es.emr.MyRegistrator
import um.re.es.emr.NumberFinder2
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

object PrepareData {
  val conf_s = new SparkConf().setAppName("ginger").set("master", "yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)
  val source = sc.textFile("s3://touchbeam-datascience/12_6_2014")
  val counts = source.flatMap { l => l.split(" ") }.map(word => (word, 1)).reduceByKey(_ + _)
  val source4 = sc.textFile("s3://touchbeam-datascience/archivefile3.zip")
}

