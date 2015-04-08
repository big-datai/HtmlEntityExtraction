package um.re.analysis

import scala.collection.JavaConversions.mapAsScalaMap
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.elasticsearch.hadoop.mr.EsInputFormat
import um.re.data.DataSchema
import um.re.utils.EsUtils

class UConfAnal(sc: SparkContext, parts: Int) {
  val conf = new JobConf()
  conf.set("es.resource", EsUtils.ESINDEXANAL)
  conf.set("es.nodes", EsUtils.ESIPANAL)

  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val all = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString()) }.toMap) }.repartition(parts)

  def getData() = {
    all
  }

  def setQuery(query: String) {
    conf.set("", "")
    conf.set("es.query", "?q=" + query)
  }

}
