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
import um.re.utils.EsUtils

/**
 * This class is supposed to read and write data to ES and analyse it on EMR cluster
 */

object General extends App {



  val reg = new URegistrator

  //val conf = new Configuration()
  val conf = new JobConf()
  conf.set("es.resource", "htmls/data")
  conf.set("es.query", "?q=prod_id:23799864")
  conf.set("es.nodes", EsUtils.ESIP)

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  //val sc = new SparkContext(conf_s)
  
  //Run a query
  //val conf_s = new SparkConf().setAppName("es").setMaster("local[8]").set("spark.serializer", classOf[KryoSerializer].getName)
  conf_s.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf_s.set("spark.kryo.registrator", "um.re.es.emr.MyRegistrator")
  conf_s.set("es.index.auto.create", "true")
  conf_s.set("es.resource", "htmls/data")
  conf_s.set("es.query", "?q=prod_id:23799864")
  conf_s.set("es.nodes", EsUtils.ESIP)
  
  
  val sc = new SparkContext(conf_s)

  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  
  val source2 = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString) }.toMap) }.repartition(100)
  source2.partitions.size

  val source3 = source.map { l => (l._1.toString(), l._2.toString) }.repartition(100)

  val source15 = sc.makeRDD(source2.take(15))
  val candid15 = source15.map { l =>
    try {
      val nf = PriceParcer
      val id = l._2.get("url").toString
      val h = l._2.get("price_prop1").toString
      val res = nf.find(id, h)
      res
    } catch {
      case _: Exception => { "[{\"no\":\"data\"}]" }
    }
  }

  val one = source2.take(1)
  
  
  val m = one.apply(0)._2.filterKeys(p =>
    if (p == "url" || p == "price_prop1" || p == "price_patterns")
      true
    else
      false)

  val url=m.get("url")    
  val pat=m.get("price_patterns")
  val html=m.get("price_prop1")
  
  source.saveAsTextFile("hdfs:///spark-logs//raw1")
  source.coalesce(1, true).saveAsTextFile("hdfs:///spark-logs//raw")
  //http://wpcertification.blogspot.co.il/2014/08/how-to-use-elasticsearch-as-input-for.html
  val temp2 = sc.textFile("hdfs:///spark-logs/docs2/part-00000")

  val temp = sc.textFile("")

  Utils.printToFile(new File("//mnt//res.txt"))(p => {
    temp2.toArray.foreach(p.println)
  })

  
}