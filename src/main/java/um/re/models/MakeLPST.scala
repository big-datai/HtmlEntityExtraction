package um.re.models

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.hadoop.io.MapWritable
import org.apache.spark.SparkConf
import org.apache.hadoop.mapred.JobConf
import org.elasticsearch.hadoop.mr.EsInputFormat
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
import org.apache.spark.mllib.classification.SVMWithSGD

object MakeLPST extends App {

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val conf = new JobConf()
  conf.set("es.resource", "candidl/data")
  conf.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")
  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val all = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString()) }.toMap) }.repartition(100)
  //merge text before and after

  val parsedData = all.map { l =>
    val before = Utils.tokenazer(l._2.apply("text_before"))
    val after = Utils.tokenazer(l._2.apply("text_after"))
    val domain = Utils.getDomain(l._2.apply("url"))
    val location = l._2.apply("location")
    val d1 = before ++ after
    //TF CALCULATION
    val d_tf: TrieMap[String, Double] = new TrieMap
    d1.foreach { ll =>
      if (d_tf.putIfAbsent(ll, 1) != None) {
        d_tf.update(ll, d_tf.apply(ll) + 1)
      }
    }

    val parts = d1 ++ Array(domain) //, location)
    //Precision = 0.8691345120351838
    val parts_embedded = parts.filter { w => (!w.isEmpty() && w.length > 3) && w.length < 20 && d_tf.lookup(w) > 0 && (d_tf.apply(w) == 1 || d_tf.apply(w) > 4) }.map { w => w.toLowerCase }
    if ((l._2.get("priceCandidate").get.toString.contains(l._2.get("price").get.toString)))
      (1, parts_embedded)
    else
      (0, parts_embedded)
  }.filter(l => l._2.length > 1)
  parsedData.take(10).foreach(println)

  val docs = parsedData.map(l => l._2).flatMap(f => f)

  val documents_p: RDD[Seq[String]] = parsedData.filter(l => l._1 == 1).map(l => l._2)
  //POSITIVE
  val hashingTF = new HashingTF(50000)
  val tf: RDD[Vector] = hashingTF.transform(documents_p)

  import org.apache.spark.mllib.feature.IDF

  val idf = (new IDF(minDocFreq = 10)).fit(tf)
  val tfidf: RDD[Vector] = idf.transform(tf)
  //NEGATIVE
  val documents_n: RDD[Seq[String]] = parsedData.filter(l => l._1 == 0).map(l => l._2)

  val hashingTFn = new HashingTF(50000)
  val tfn: RDD[Vector] = hashingTFn.transform(documents_n)

  val idfn = (new IDF(minDocFreq = 10)).fit(tfn)
  val tfidfn: RDD[Vector] = idfn.transform(tfn)

  val positive = tfidf.map { l => LabeledPoint(1, l) }
  val negat = tfidfn.map { l => LabeledPoint(0, l) }
  //ALL TOGETHER
  val points = positive ++ negat
  //SAVE TO FILE ALL DATA
  MLUtils.saveAsLibSVMFile(points, "hdfs:///pavlovout/pointsst")

}