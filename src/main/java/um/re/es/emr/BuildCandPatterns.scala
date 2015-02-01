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
import um.re.utils.Utils
import um.re.utils.EsUtils

object BuildCandPatterns extends App {
  val conf_s = new SparkConf().setAppName("es").setMaster("local[8]") //.set("spark.serializer", classOf[KryoSerializer].getName)
  conf_s.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf_s.set("spark.kryo.registrator", "um.re.es.emr.MyRegistrator")
  val sc = new SparkContext(conf_s)

  val conf = new JobConf()
  conf.set("es.resource", "htmls/data")
  conf.set("es.query", "?q=price_prop1:xml") //"?q=prod_id:23799864") //
  conf.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")

  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])

  val source2 = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString) }.toMap) }.repartition(100)
  val source15 = sc.makeRDD(source2.take(15))

  val res = Utils.getCandidatesPatternsHtmlTrimed(source2)

  //res.take(1).foreach(println)
  //val one = sc.makeRDD(res.take(1))

  val db = res.filter(l => l != null).map { l =>
    try {
      val map_pat = l.head
      val pat = map_pat.get("patterns").get.toString
      val html = map_pat.get("html").get.toString
      val location_pattern = Utils.allPatterns(pat, html, 150)
      //add to each candidate pattern
      l.tail.map { cand =>
        cand + ("patterns" -> location_pattern.mkString("|||")) + ("price" -> map_pat.get("price").get.toString)
      }
    } catch {
      case _: Exception => null
    }
  }.filter(l => l != null)

  val fin = db.flatMap(l => l)

  val conf2 = new JobConf()
  conf2.set("es.resource", "candidates/data")
  //conf.set("es.query", "?q=price_prop1:xml") //"?q=prod_id:23799864") //
  conf2.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")
  EsUtils.write2ESHadoopMap(fin, conf2)

  fin.take(1).foreach(println)

}