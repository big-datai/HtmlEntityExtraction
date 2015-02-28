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
import org.apache.spark.sql.{ Row, SQLContext }

object DataExploer extends App {

  def map2JsonString(map: Map[String, String]) = {
    val asJson = Json.toJson(map)
    Json.stringify(asJson)
  }

  def string2Json(jsonString: String) = {
    Json.parse(jsonString)
  }

  val conf_s = new SparkConf().setAppName("soomla")
  val sc = new SparkContext(conf_s)

  val source = sc.textFile("s3://rawd/soomla/events.json")

  //validate jsons
  val data = source.map { l =>
    try {
      string2Json(l).toString
    } catch { case _: Exception => null }
  }//.filter(l => (l != null && l.contains("Android"))).cache

  
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext._
  // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
  import sqlContext.createSchemaRDD
  val data_sql = sqlContext.jsonRDD(data)
  data_sql.printSchema()

  data_sql.registerTempTable("soomla")
  sqlContext.cacheTable("soomla")

  val s = sqlContext.sql("SELECT distinct name FROM soomla limit 100")
  
  //__v#0,_id#1,country_code#2,device_id#3,extra_info#4,name#5,new_event#6,platform#7,server_date#8L,storefront_id#9,time_millis#10L
 /*
  val p = sqlContext.sql("SELECT name, country_code, device_id, count(new_event) FROM soomla WHERE country_code is not NULL GROUP BY name, country_code, device_id")
  val p = sqlContext.sql("SELECT distinct name  FROM soomla")

  val p = sqlContext.sql("SELECT * from soomla WHERE  name = 'item_purchase'")
  val p = sqlContext.sql("SELECT * from soomla WHERE  name = 'market_purchase'")
  p.take(100).foreach(println)
  
  val p = sqlContext.sql("SELECT * FROM soomla")
  
  val fin = data_sql.select('source, 'version, 'Country).take(10).foreach {
    case Row(source: String, version: String, country: String) =>
      println(source)
  }
*/
}