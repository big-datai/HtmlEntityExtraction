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
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.{ Row, SQLContext }
object PrepareData {

  def map2JsonString(map: Map[String, String]) = {
    val asJson = Json.toJson(map)
    Json.stringify(asJson)
  }

  def string2Json(jsonString: String) = {
    Json.parse(jsonString)
  }
  val conf_s = new SparkConf().setAppName("ginger").set("master", "yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)
  val source = sc.textFile("s3://touchbeam-datascience/trFromFrontEndServers*2015")
  //PREPARE DATA FOR SQL
  val data = source.map { l =>
    try {
      string2Json(l).toString
    } catch { case _: Exception => null }
  }.filter(l => (l != null && l.contains("Android"))).cache

  val events = sc.makeRDD(data.take(1)).map { l =>
    try {
      val json = string2Json(l)
      json.apply(0)
    } catch { case _: Exception => null }
  }.filter(l => (l != null))

  data.saveAsTextFile("s3://rawd/gingercompressed", classOf[GzipCodec])

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext._
  // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
  import sqlContext.createSchemaRDD
  val data_sql = sqlContext.jsonRDD(data)
  data_sql.printSchema()

  data_sql.registerTempTable("raw")
  sqlContext.cacheTable("raw")

  val s = sqlContext.sql("SELECT * FROM raw limit 10")

  val fin = data_sql.select('source, 'version, 'Country).take(10).foreach {
    case Row(source: String, version: String, country: String) =>
      println(source)
  }

}

