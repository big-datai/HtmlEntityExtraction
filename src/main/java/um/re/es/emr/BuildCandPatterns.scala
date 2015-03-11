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

object BuildCandPatterns extends App {
  val conf_s = new SparkConf().setAppName("es").setMaster("yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  conf_s.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf_s.set("spark.kryo.registrator", "um.re.es.emr.URegistrator")
  val sc = new SparkContext(conf_s)

  val conf = new JobConf()
  conf.set("es.resource", "htmls/data")
  conf.set("es.query", "?q=price_prop1:xml")
  conf.set("es.nodes", EsUtils.ESIP)

  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val source2 = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString) }.toMap) }.repartition(300)
  val res = Utils.getCandidatesPatternsHtmlTrimed(source2)

  val db = res.filter { l =>
    val map_pat = l.head
    var count = 0;
    l.tail.map { cand =>
        if (cand.get("priceCandidate").get.toString.contains(map_pat.get("price").get.toString))
          count = count + 1;
      }
    l != null && count > 0
  }.map { l =>
      try {
        val map_pat = l.head
        val pat = map_pat.get("patterns").get.toString
        val html = map_pat.get("html").get.toString
        val length=html.size
        val location_pattern = Utils.allPatterns(pat, html, 150)
        //add to each candidate pattern
        l.tail.map { cand =>
          // cand + ("patterns" -> location_pattern.mkString("|||")) + ("price" -> map_pat.get("price").get.toString)
          cand + ("price" -> map_pat.get("price").get.toString) + ("patterns" -> Utils.map2JsonString(location_pattern))+("length"->length.toString)
          //cand  + ("price" -> map_pat.get("price").get.toString)+ ("patterns" -> location_pattern.toSeq)
        }
      } catch {
        case _: Exception => null
      }
    }.filter(l => l != null)

  val fin = db.flatMap(l => l)

  val conf2 = new JobConf()
  conf2.set("es.resource", "candidl5/data")
  conf2.set("es.nodes", EsUtils.ESIP)
  EsUtils.write2ESHadoopMap(fin, conf2)

  fin.take(1).foreach(println)

}