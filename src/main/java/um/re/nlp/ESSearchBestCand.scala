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

object ESPatternAlgo {
  //val conf = new Configuration()
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)

  val conf = new JobConf()
  conf.set("es.resource", EsUtils.ESINDEX)
  conf.set("es.nodes", EsUtils.ESIP)

  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val source_map = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString) }.toMap) }.repartition(100)

  val queryPrice = source_map.map { l =>
    try {
      val patterns = l._2.get("patterns").get.toString
      //Utils.string2Json(patterns)
      val query = patterns.replaceAll("[^0-9A-Za-z]+", " ").replaceAll("[\\p{Blank}]{2,}", " ").split(" ").distinct.mkString(" ") //map(w=>(w,1)).reduceByKey(_+_)
      val price_o = l._2.get("price").get.toString
      (query, price_o)
    } catch {
      case _: Exception => null
    }
  }

  val arr_patterns = queryPrice.take(1) //toArray

  //TODO USE REGULAR JAVA CONNECTOR TO RUN A QUERY THIS ONE DOES NOT SORT THE RESULTS
  def search(query: String, price_o: String) = {
    val cnf = new JobConf()
    cnf.set("es.resource", EsUtils.ESINDEX)
    cnf.set("es.nodes", EsUtils.ESIP)
    val b = """{"query":{"bool":{"must":[{"query_string":{"default_field":"_all","query":"""
    val a = """}}],"must_not":[],"should":[]}},"from":0,"size":1,"sort":[],"facets":{}}"""
    val q = b + "\"" + query + "\"" + a

    val q5 = """{
    "query": {
        "match": {
            "_all": " 35559 v script type text javascript language JavaScript 1 2 CDATA window calcPriceData priceTo productX 35 000 productY 18 price 593 70 productInBox product avail 119 div id browse form name orderform method post action cart php mode add 36561 span h2 li dl class each dt pr TaxedPrice1 Sale Price dd TaxedPrice priceto Market alt 771 81 You Save"
        }
    }
}"""
    cnf.set("es.query", q5)   
    val res = sc.newAPIHadoopRDD(cnf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
    val res_map = res.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString) }.toMap) }
    val prices = res_map.take(10).map { p => p._2.get("price").get.toString }
    val price_cand = res_map.take(1).apply(0)._2.get("price").get.toString
    (price_cand, res_map.count, res_map.take(1).apply(0)._2.get("length").get.toString)
  }

  val res = arr_patterns.map { l => search(l._1, l._2) }

}
