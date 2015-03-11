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
import org.apache.spark.mllib.feature.IDFModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import um.re.utils.EsUtils

object RandForestFeatures extends App {

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  def data2points(data: RDD[(Int, Seq[String], Double)], idf_vector: Array[Double], hashingTF: HashingTF) = {
    val idf_vals = idf_vector
    val tf_model = hashingTF
    data.map {
      case (lable, txt, location) =>
        val tf_vals = tf_model.transform(txt).toArray
        val features = tf_vals
        val values = features.filter { l => l != 0 }
        val index = features.zipWithIndex.filter { l => l._1 != 0 }.map { l => l._2 }
        LabeledPoint(lable, Vectors.sparse(features.length, index, values))

    }
  }

  def filterData(data: RDD[LabeledPoint], unified_indx_idf: (Array[Int], Array[Double])) = {
    val idf_vals = unified_indx_idf._2
    val unified_indx = unified_indx_idf._1
    data.map { point =>
      val label1 = point.label
      val tf_val = point.features.toArray
      val tf_vals_uniq = unified_indx.map(i => tf_val(i))
      val tfidf_vals = (tf_vals_uniq, idf_vals).zipped.map((d1, d2) => d1 * d2)
      val features = tfidf_vals
      val values = features.filter { l => l != 0 }
      val index = features.zipWithIndex.filter { l => l._1 != 0 }.map { l => l._2 }
      LabeledPoint(label1, Vectors.sparse(features.length, index, values))
    }
  }
  def selectTopK(k: Int, tf: RDD[Vector], idf_vector: Array[Double], idf_model: IDFModel) = {
	  //k is number of tdudf features
	  val tfidf_stats = Statistics.colStats(idf_model.transform(tf))
			  val tfidf_avg = tfidf_stats.mean.toArray
			  val tfidf_avg_sorted = tfidf_avg.clone
			  //  val tfidf_avg_sorted = tfidf_stats.mean.toArray
			  java.util.Arrays.sort(tfidf_avg_sorted)
			  val top_k_value = tfidf_avg_sorted.takeRight(k)(0)
			  val selected_indices = (for (i <- tfidf_avg.indices if tfidf_avg(i) >= top_k_value) yield i).toArray
			  (selected_indices, selected_indices.map(i => idf_vector(i)))
  }

  val conf = new JobConf()
  conf.set("es.resource", EsUtils.ESINDEX)
  conf.set("es.nodes", EsUtils.ESIP)
  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val all_all = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString()) }.toMap) }.repartition(300)

  //merge text before and after
  //Split data for testing
  val sp = all_all.randomSplit(Array(0.9, 0.1), seed = 1234)
  val (no_keeping, keeping) = (sp(0), sp(1))
  val all = keeping
  //val all=all_all

  val parsedData = all.map { l =>
    val before = Utils.tokenazer(l._2.apply("text_before"))
    val after = Utils.tokenazer(l._2.apply("text_after"))
    val domain = Utils.getDomain(l._2.apply("url"))
    val location = Integer.valueOf(l._2.apply("location")).toDouble/(Integer.valueOf(l._2.apply("length")).toDouble)
    val parts = before ++ after ++ Array(domain) //, location) 
    val parts_embedded = parts 
    if ((l._2.apply("priceCandidate").contains(l._2.apply("price"))))
      (1, parts_embedded, location)
    else
      (0, parts_embedded, location)
  }.filter(l => l._2.length > 1)

  val splits = parsedData.randomSplit(Array(0.7, 0.3))
  val (trainingData, test) = (splits(0), splits(1))

  //trainng idf
  val hashingTF = new HashingTF(300000)
  val tf_pos: RDD[Vector] = hashingTF.transform(trainingData.filter(t => t._1 == 1).map(l => l._2))
  val tf_neg: RDD[Vector] = hashingTF.transform(trainingData.filter(t => t._1 == 0).map(l => l._2))
  val idf_pos = (new IDF(minDocFreq = 10)).fit(tf_pos)
  val idf_vector_pos = idf_pos.idf.toArray
  val idf_neg = (new IDF(minDocFreq = 10)).fit(tf_neg)
  val idf_vector_neg = idf_neg.idf.toArray

  val tf: RDD[Vector] = hashingTF.transform(trainingData.map(l => l._2))
  val idf = (new IDF(minDocFreq = 10)).fit(tf)
  val idf_vector = idf.idf.toArray


  val tf_plus_idf_pos = selectTopK(100, tf_pos, idf_vector_pos, idf_pos)
  val tf_plus_idf_neg = selectTopK(100, tf_neg, idf_vector_neg, idf_neg)
  val ind_pos = tf_plus_idf_pos._1.toSet
  val ind_neg = tf_plus_idf_neg._1.toSet
  val uniq_ind_neg = (ind_neg -- ind_pos).toArray
  val uniq_val_neg = uniq_ind_neg.map(i => idf_vector_neg(i))
  val uniq_ind_val_neg = (uniq_ind_neg, uniq_val_neg)
  val unified_indx_idf = (tf_plus_idf_pos._1 ++ uniq_ind_neg, tf_plus_idf_pos._2 ++ uniq_val_neg)
  val tf_tarin: RDD[Vector] = hashingTF.transform(trainingData.map(l => l._2))
  val data = data2points(trainingData, idf_vector, hashingTF)
  val training_points = filterData(data, unified_indx_idf)

  //Building test set
  val data_test = data2points(test, idf_vector, hashingTF)
  val test_points = filterData(data_test, unified_indx_idf)

  val treeStrategy = Strategy.defaultStrategy("Classification")
  val numTrees = 3 // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val model = RandomForest.trainClassifier(training_points,
    treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)

  // Evaluate model on test instances and compute test error
  def labelAndPred(input_points: RDD[LabeledPoint]) = {
    val local_model = model
    val labelAndPreds = input_points.map { point =>
      val prediction = local_model.predict(point.features)
      (point.label, prediction)
    }
    labelAndPreds
  }
  //Test model on training data
  val labelAndPreds_train = labelAndPred(training_points)
  val tp_trn = labelAndPreds_train.filter { case (l, p) => (l == 1) && (p == 1) }.count
  val tn_trn = labelAndPreds_train.filter { case (l, p) => (l == 0) && (p == 0) }.count
  val fp_trn = labelAndPreds_train.filter { case (l, p) => (l == 0) && (p == 1) }.count
  val fn_trn = labelAndPreds_train.filter { case (l, p) => (l == 1) && (p == 0) }.count
  //Test model on test data
  val labelAndPreds_test = labelAndPred(test_points)
  val tp_tst = labelAndPreds_test.filter { case (l, p) => (l == 1) && (p == 1) }.count
  val tn_tst = labelAndPreds_test.filter { case (l, p) => (l == 0) && (p == 0) }.count
  val fp_tst = labelAndPreds_test.filter { case (l, p) => (l == 0) && (p == 1) }.count
  val fn_tst = labelAndPreds_test.filter { case (l, p) => (l == 1) && (p == 0) }.count
}