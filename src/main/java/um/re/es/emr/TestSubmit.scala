package um.re.es.emr

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
import um.re.utils
import um.re.utils.Utils
object TestSubmit extends App {

  println("Hello from submit")

  val reg = new URegistrator

  //val conf = new Configuration()
  val conf = new JobConf()
  conf.set("es.resource", "htmls/data")
  conf.set("es.query", "?q=prod_id:23799864")
  conf.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  //val sc = new SparkContext(conf_s)

  //Run a query
  //val conf_s = new SparkConf().setAppName("es").setMaster("local[8]").set("spark.serializer", classOf[KryoSerializer].getName)
  conf_s.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //conf_s.set("spark.kryo.registrator", "um.re.es.emr.URegistrator")
  conf_s.set("es.index.auto.create", "true")
  conf_s.set("es.resource", "html/data")
  conf_s.set("es.query", "?q=prod_id:23799864")
  conf_s.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")
  val sc = new SparkContext(conf_s)

  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])

  //  val source2 = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString) }.toMap) }.repartition(100)
  //println(source2.partitions.size)

  //val source3 = source.map { l => (l._1.toString(), l._2.toString) }.repartition(100)

  val cfg = Map("es.nodes" -> "ec2-54-167-216-26.compute-1.amazonaws.com", "es.resource" -> "aaa/data", "es.index.auto.create" -> "true", "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer")
  val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
  val airports = Map("arrival" -> "Otopeni", "SFO" -> Seq(numbers, numbers))
  EsSpark.saveToEs(sc.makeRDD(Seq(numbers, airports)), cfg)
}