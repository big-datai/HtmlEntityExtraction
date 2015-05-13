package um.re.emr

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
import um.re.utils.URegistrator
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
import um.re.utils.PriceParcer
import um.re.utils.PriceParser

object BuildCandidTest extends App {
  val conf_s = new SparkConf() //.setAppName("es").setMaster("yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  //conf_s.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //conf_s.set("spark.kryo.registrator", "um.re.es.emr.URegistrator")
  val sc = new SparkContext(conf_s)

  val conf = new JobConf()
  conf.set("es.resource", "htmls/data")
  conf.set("es.query", "?q=price_prop1:xml")
  conf.set("es.nodes", EsUtils.ESIP)

  def getCandidatesPatternsHtmlTrimed(source2: RDD[(String, Map[String, String])]): RDD[List[Map[String, String]]] = {
    val candid = source2.map { l =>
      try {
        val nf = PriceParser
        nf.snippetSize = 150
        val id = l._2.get("url").get
        val price = l._2.get("price").get
        val price_updated = l._2.get("price_updated").get
        val html = Utils.shrinkString(l._2.get("price_prop1").get)
        /*
        val html_to=l._2.get("price_prop1").get
        val m_webClient = new WebClient()
        val p=m_webClient.getPage(id)
        */
        val patterns = Utils.shrinkString(l._2.get("price_patterns").get)
        val res :List[Map[String,String]] = nf.findFast(id, html)
        val p_h = Map("patterns" -> patterns, "html" -> html, "price" -> price, "price_updated" -> price_updated)
        p_h :: res
      } catch {
        case _: Exception => Nil
      }
    }
    candid
  }
  
  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable]).repartition(90)
  val source2 = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString) }.toMap) } //sample(false, 0.01, 12345)
  val res = getCandidatesPatternsHtmlTrimed(source2)
  
  val db_filtered = res.filter { l =>
    val map_pat = l.head
    val count = l.tail.filter { cand =>
      (Utils.isTrueCandid(map_pat, cand))
    }.size
    l != null && count > 0
  }
  
  val db = db_filtered.map { l =>
    try {
      val map_pat = l.head
      val pat = map_pat.get("patterns").get.toString
      val html = map_pat.get("html").get.toString
      val length = html.size
      val location_pattern :Map[String,String]= Map.empty//Utils.allPatterns(pat, html, 150)
      //add to each candidate pattern
      l.tail.map { cand =>
        cand + ("price_updated" -> map_pat.get("price_updated").get.toString) + ("price" -> map_pat.get("price").get.toString) +
          ("patterns" -> Utils.map2JsonString(location_pattern)) + ("length" -> length.toString)
      }
    } catch {
      case _: Exception => null
    }
  }.filter(l => l != null)

  val fin = db.flatMap(l => l)
  fin.count
  val conf2 = new JobConf()
  conf2.set("es.resource", "candidl/data")
  conf2.set("es.nodes", EsUtils.ESIP)
  EsUtils.write2ESHadoopMap(fin, conf2)

  fin.take(1).foreach(println)

}