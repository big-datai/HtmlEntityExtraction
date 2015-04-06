package um.re.utils

import scala.collection.JavaConversions.mapAsScalaMap
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.elasticsearch.hadoop.mr.EsInputFormat
import um.re.data.DataSchema

class UConf(sc: SparkContext, parts: Int) {
  val conf = new JobConf()
  conf.set("es.resource", EsUtils.ESINDEX)
  conf.set("es.nodes", EsUtils.ESIP)

  def getData() = {
    val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
    val all = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString()) }.toMap) }.repartition(parts)
    all
  }

  def getDataFS(path: String = Utils.HDFSSTORAGE + Utils.DCANDIDS) = {
    sc.objectFile[(String, Map[String, String])](path, parts)
  }

  def setQuery(query: String) {
    conf.set("", "")
    conf.set("es.query", "?q=" + query)
  }

}
