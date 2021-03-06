package um.re.emr

import com.utils.messages.BigMessage
import org.apache.hadoop.io.{MapWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.hadoop.mr.EsInputFormat
import um.re.utils.{EsUtils, Utils}

import scala.collection.JavaConversions._

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
    val msgEmptyHtml = BigMessage.string2Message(jsStr)
    msgEmptyHtml.sethtml("")
    (msgEmptyHtml.toJson().toString().getBytes(), dataMap)
  }.repartition(300) //sample(false, 0.001, 12345)
  val db = Utils.htmlsToCandidsPipe(source2)
  val fin = db.flatMap { case (msg, l) => l }
  if (Utils.DEBUGFLAG)
    fin.count
  val conf2 = new JobConf()
  conf2.set("es.resource", "candidl/data")
  conf2.set("es.nodes", EsUtils.ESIP)
  EsUtils.write2ESHadoopMap(fin, conf2)

  fin.take(1).foreach(println)

}