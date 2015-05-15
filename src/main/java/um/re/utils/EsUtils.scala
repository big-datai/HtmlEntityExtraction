package um.re.utils
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

object EsUtils {
  val ESIP = "107.20.157.48"
  val ESINDEX = "full_river2/data"
  val conf = new JobConf()
  conf.set("es.resource", "process_count/counter")
  conf.set("es.query", "?q=updatePriceCount")
  conf.set("es.nodes", ESIP)
  conf.set("es.index.auto.create", "true")

  /**
   * This method writes to ES using elasticsearch.spark
   * this method get rdd of map object and saves them into specified index
   */
  def write2ES(exit: RDD[Map[String, String]], index: String) {
    val ind = index + "/data"
    val cfg = Map("es.nodes" -> EsUtils.ESIP, "es.resource" -> ind,
      "es.index.auto.create" -> "true", "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer")
    // sample how it works
    //val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    //val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fransco")    
    // EsSpark.saveToEs(sc.makeRDD(Seq(numbers, airports)), cfg)
    EsSpark.saveToEs(exit, cfg)
  }

  def copyData(index_source: String, index_dest: String, sc: SparkContext) {
    val conf1 = new JobConf()
    conf1.set("es.resource", index_source + "/" + "data")
    conf1.set("es.nodes", EsUtils.ESIP)

    val source = sc.newAPIHadoopRDD(conf1, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
    val conf2 = new JobConf()
    conf2.set("es.resource", index_dest + "/" + "data")
    conf2.set("es.nodes", EsUtils.ESIP)

    source.saveAsNewAPIHadoopFile("-", classOf[NullWritable], classOf[MapWritable], classOf[EsOutputFormat], conf2)
  }
  /**
   * This method should write to ES using hadoop style
   */
  //http://www.elasticsearch.org/guide/en/elasticsearch/hadoop/current/mapreduce.html
  def write2ESHadoop(source: RDD[(String, Map[String, String])], cfg: JobConf) = {
    source.map(r =>
      (NullWritable.get, Utils.toWritable(r._2))).saveAsNewAPIHadoopFile("-", classOf[NullWritable], classOf[MapWritable], classOf[EsOutputFormat], cfg)
  }

  def write2ESHadoopMap(source: RDD[Map[String, String]], cfg: JobConf) = {
    source.map(r =>
      (NullWritable.get, Utils.toWritable(r))).saveAsNewAPIHadoopFile("-", classOf[NullWritable], classOf[MapWritable], classOf[EsOutputFormat], cfg)
  }
  def es2s3(esName: String, sc: SparkContext) {
    val conf3 = new JobConf()
    conf3.set("es.resource", esName + "/data")
    conf3.set("es.nodes", EsUtils.ESIP)

    val source7 = sc.newAPIHadoopRDD(conf3, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable]).cache

    source7.map { l =>
      val map = Utils.mapWritableToInput(l._2)
      val asJson = Json.toJson(map)
      Json.stringify(asJson) + "," + System.lineSeparator()
    }.coalesce(100, true).saveAsTextFile("s3://pavlovout/" + esName)

  }
}
