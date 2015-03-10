package um.re.models

import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.serializer.KryoSerializer
import scala.collection.JavaConversions._
import play.api.libs.json._
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.apache.spark.SparkContext
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.spark.serializer.KryoSerializer
import um.re.utils.Utils
import um.re.utils.EsUtils
//import org.elasticsearch.hadoop.mr.EsInputFormat[org.apache.hadoop.io.Text,org.apache.hadoop.io.{EsInputFormat => MapWritable]}

object ESPatternAlgo {
  //val conf = new Configuration()
  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val conf = new JobConf()
  conf.set("es.resource", "candidl/data")
  conf.set("es.nodes", EsUtils.ESIP)
  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val source_map = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString) }.toMap) }.repartition(100)

  source_map.saveAsObjectFile("hdfs:///user/obj")
  var acc = sc.accumulable(0)
  val patterns = source_map.map { l =>
    try {
      val patterns = l._2.get("patterns").get.toString
      //Utils.string2Json(patterns)
      val query = patterns.replaceAll("[^A-Za-z]+", " ").replaceAll("[\\p{Blank}]{2,}", " ").split(" ").distinct.mkString(" ") //map(w=>(w,1)).reduceByKey(_+_)
      val price_o = l._2.get("price").get.toString
      (query, price_o)
    } catch {
      case _: Exception => null
    }
  }
  val patterns2 = source_map.map { l =>
    try {
      val patterns = l._2.get("patterns").get.toString
      //Utils.string2Json(patterns)
      val query = patterns.replaceAll("[^A-Za-z]+", " ").split(" ")
      val price_o = l._2.get("price").get.toString

      val cnf = new JobConf()
      cnf.set("es.nodes", EsUtils.ESIP)
      cnf.set("es.query", "?q=text_before:" + query)
      val res = sc.newAPIHadoopRDD(cnf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
      val res_map = res.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString) }.toMap) }
      val price_cand = res_map.take(1).apply(0)._2.get("price").get.toString

      if (price_o == price_cand)
        acc.add(1)

    } catch {
      case _: Exception => null
    }
  }

  val arr_patterns = patterns.take(10) //toArray

  val cnf = new JobConf()
  cnf.set("es.resource", "candidl/data")
  cnf.set("es.nodes", EsUtils.ESIP)
  cnf.set("es.query", "?q=" + "xml")
  arr_patterns.foreach { l =>
    val query = l._1
    val price_o = l._2
    val res = sc.newAPIHadoopRDD(cnf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
    val res_map = res.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString) }.toMap) }
    val price_cand = res_map.take(1).apply(0)._2.get("price").get.toString
    if (price_o == price_cand) {
      acc.add(1)
    }
  }

  acc.value
}