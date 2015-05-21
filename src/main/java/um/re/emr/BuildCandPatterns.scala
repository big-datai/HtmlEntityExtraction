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
import com.utils.messages.MEnrichMessage

object BuildCandPatterns extends App {
  val conf_s = new SparkConf() //.setAppName("es").setMaster("yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  //conf_s.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //conf_s.set("spark.kryo.registrator", "um.re.es.emr.URegistrator")
  val sc = new SparkContext(conf_s)

  val conf = new JobConf()
  conf.set("es.resource", "htmls/data")
  conf.set("es.query", "?q=html:xml")
  conf.set("es.nodes", EsUtils.ESIP)

  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val source2 = source.map { l =>
    val dataMap = l._2.map { case (k, v) => (k.toString, v.toString) }.toMap
    val jsStr = Utils.map2JsonString(dataMap).toString()
    val msgEmptyHtml = MEnrichMessage.string2Message(jsStr)
    msgEmptyHtml.sethtml("")
    (msgEmptyHtml.toJson().toString().getBytes(),dataMap)}.repartition(300) //sample(false, 0.001, 12345)
  val db = Utils.htmlsToCandidsPipe(source2)
  val fin = db.flatMap{case(msg,l) => l}
    if (Utils.DEBUGFLAG)
      fin.count
  val conf2 = new JobConf()
  conf2.set("es.resource", "candidl/data")
  conf2.set("es.nodes", EsUtils.ESIP)
  EsUtils.write2ESHadoopMap(fin, conf2)

  fin.take(1).foreach(println)

}