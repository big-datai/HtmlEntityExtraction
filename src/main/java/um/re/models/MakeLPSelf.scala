package um.re.models

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.mapAsScalaMap
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.serializer.KryoSerializer
import um.re.utils.Utils
import org.elasticsearch.hadoop.mr.EsInputFormat
import scala.collection.concurrent.TrieMap
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.util.MLUtils
import play.api.libs.json.JsObject
import play.api.libs.json._

object MakeLPSelf extends App {

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  def getInd(term: String, size: Int) = {
    math.abs(term.hashCode).toInt % size
  }
  val conf = new JobConf()
  conf.set("es.resource", "candidl/data")
  conf.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")
  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])

  val all =source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString()) }.toMap) }.repartition(100)

  val parsedData = all.map { l =>
    val before = Utils.tokenazer(l._2.apply("text_before")).map { l =>
      math.abs(l.hashCode).toInt % 50000
    }.toArray
    val after = Utils.tokenazer(l._2.apply("text_after")).map { l =>
      math.abs(l.hashCode).toInt % 50000
    }.toArray
    val parts = before ++ after
    if ((l._2.get("priceCandidate").get.toString.contains(l._2.get("price").get.toString)))
      (1, parts)
    else
      (0, parts)
  }.filter(l => l._2.length > 1)

  val flat = parsedData.flatMap(f => f._2).distinct.count

  val points = parsedData.map { l =>
  val size = l._2.size
  //TF CALCULATION
  val d_tf: TrieMap[Int, Double] = new TrieMap
  l._2.foreach { ll =>
    if (d_tf.putIfAbsent(ll, 1) != None) {
      d_tf.update(ll, d_tf.apply(ll) + 1)
    }
  }
  val location = d_tf.map { l => l._1 }.toArray
  val values = d_tf.map { l => l._2 }.toArray
  LabeledPoint(l._1, Vectors.sparse(50000, location, values))
  }
   //SAVE TO FILE ALL DATA
  MLUtils.saveAsLibSVMFile(points, "hdfs:///pavlovout/points")

 
}