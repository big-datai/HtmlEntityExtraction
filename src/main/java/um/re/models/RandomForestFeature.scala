package um.re.models

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkConf
import scala.collection.JavaConversions.mapAsScalaMap
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import um.re.utils.EsUtils
import um.re.utils.UConf
import um.re.transform.Transformer

object RandomForestFeature extends App {

  val conf = new SparkConf().set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf)
  println("Hello from random forest")
  val data = new UConf(sc, 600)
  val all = data.getData
  val parsedData = Transformer.parseData(all)

  val d = Transformer.dataSample(0.1, parsedData)
  val splits = d.randomSplit(Array(0.7, 0.3))
  val (trainingData, test) = (splits(0), splits(1))

  //trainng idf
  val hashingTF = new HashingTF(300000)
  val tf: RDD[Vector] = hashingTF.transform(trainingData.map(l => l._2))
  val idf = (new IDF(minDocFreq = 10)).fit(tf)
  val idf_vector = idf.idf.toArray

  val k = 100 //number of tdudf features
  val tfidf_stats = Statistics.colStats(idf.transform(tf))
  val tfidf_avg = tfidf_stats.mean.toArray
  val tfidf_avg_sorted = tfidf_stats.mean.toArray

  val top_k_value = tfidf_avg_sorted.sorted.takeRight(k)(0)
  val selected_indices = (for (i <- tfidf_avg.indices if tfidf_avg(i) >= top_k_value) yield i).toArray

  val idf_vector_filtered = selected_indices.map(i => idf_vector(i))

  val training_points = Transformer.data2points(trainingData, idf_vector_filtered, hashingTF)

  // Train a RandomForest model.
  val treeStrategy = Strategy.defaultStrategy("Classification")
  val numTrees = 3 // Use more in practice.
  val featureSubsetStrategy = "log2" // Let the algorithm choose.
  val model = RandomForest.trainClassifier(training_points, treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)

  val test_points = Transformer.data2points(test, idf_vector_filtered, hashingTF)
  // Evaluate model on test instances and compute test error
  val res = Transformer.labelAndPred(test_points, model, "forest")
  //res.saveAsTextFile("/user/randomForest")
}