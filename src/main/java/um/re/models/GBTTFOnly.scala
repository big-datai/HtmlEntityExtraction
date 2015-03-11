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
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.feature.IDF
import um.re.utils.EsUtils

object GBTTFOnly extends App {

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val conf = new JobConf()
  conf.set("es.resource", EsUtils.ESINDEX)
  conf.set("es.nodes", EsUtils.ESIP)
  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val sp = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString()) }.toMap) }.repartition(2000).randomSplit(Array(0.1, 0.9), 1234)
  val (all, rest) = (sp(0), sp(1))
  //merge text before and after

  val parsedData = all.map { l =>
    val before = Utils.tokenazer(l._2.apply("text_before"))
    val after = Utils.tokenazer(l._2.apply("text_after"))
    val domain = Utils.getDomain(l._2.apply("url"))
    val location = Integer.valueOf(l._2.apply("location")).toDouble
    val parts = before ++ after ++ Seq(domain)
    val parts_embedded = parts //.filter { w => (!w.isEmpty() && w.length > 3) }.map { w => w.toLowerCase }
    if ((l._2.apply("priceCandidate").contains(l._2.apply("price"))))
      (1, parts_embedded, location)
    else
      (0, parts_embedded, location)
  }.filter(l => l._2.length > 1)

  val splits = parsedData.randomSplit(Array(0.7, 0.3), 1234)
  val (trainingData, test) = (splits(0), splits(1))

  //trainng idf
  val hashingTF = new HashingTF(300000)
  val trainP = trainingData.filter { l => l._1 == 1 }
  val trainN = trainingData.filter { l => l._1 == 0 }

  val tfp: RDD[Vector] = hashingTF.transform(trainP.map(l => l._2))
  val tfn: RDD[Vector] = hashingTF.transform(trainN.map(l => l._2))

  val idfp = (new IDF(minDocFreq = 10)).fit(tfp)
  val idfn = (new IDF(minDocFreq = 10)).fit(tfn)

  val idf_vectorp = idfp.idf.toArray
  val idf_vectorn = idfn.idf.toArray

  def data_to_points(idf_vals: Array[Double], data: RDD[(Int, Seq[String], Double)]) = {
    val tf_model = hashingTF
    data.map {
      case (lable, txt, location) =>
        val tf_vals = tf_model.transform(txt).toArray
        val tfidf_vals = (tf_vals, idf_vals).zipped.map((d1, d2) => d1 * d2)
        val features = tfidf_vals ++ Array(location)
        val values = features.filter { l => l != 0 }
        val index = features.zipWithIndex.filter { l => l._1 != 0 }.map { l => l._2 }
        LabeledPoint(lable, Vectors.sparse(features.length, index, values))
    }
  }

  val training_pointsp = data_to_points(idf_vectorp, trainP)
  val training_pointsn = data_to_points(idf_vectorn, trainN)
  val training_points = training_pointsp ++ training_pointsn

  val tf: RDD[Vector] = hashingTF.transform(trainingData.map(l => l._2))

  val idf = (new IDF(minDocFreq = 10)).fit(tf)
  //val tfidf = idf.transform(tf)
  val idf_vector = idf.idf.toArray
  val test_points = data_to_points(idf_vector, test)
  val boostingStrategy =
    BoostingStrategy.defaultParams("Classification")
    
  boostingStrategy.numIterations = 3 // Note: Use more in practice
  //boostingStrategy.treeStrategy.maxDepth = 2
  val model =
    GradientBoostedTrees.train(training_points, boostingStrategy)

  // Evaluate model on test instances and compute test error
  def labelAndPred(input_points: RDD[LabeledPoint]) = {
    val local_model = model
    val labelAndPreds = input_points.map { point =>
      val prediction = local_model.predict(point.features)
      (point.label, prediction)
    }
    labelAndPreds
  }

  val labelAndPreds = labelAndPred(test_points)
  val tp = labelAndPreds.filter { case (l, p) => (l == 1) && (p == 1) }.count
  val tn = labelAndPreds.filter { case (l, p) => (l == 0) && (p == 0) }.count
  val fp = labelAndPreds.filter { case (l, p) => (l == 0) && (p == 1) }.count
  val fn = labelAndPreds.filter { case (l, p) => (l == 1) && (p == 0) }.count

  println("tp : " + tp + ", tn : " + tn + ", fp : " + fp + ", fn : " + fn)
  println("sensitivity : " + tp / (tp + fn).toDouble + " specificity : " + tn / (fp + tn).toDouble + " precision : " + tp / (tp + fp).toDouble)

}