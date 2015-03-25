package um.re.models

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
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
import org.apache.spark.mllib.tree.model.RandomForestModel
import um.re.utils.Utils

object RandomForestFeature extends App {

  val conf_s = new SparkConf().setAppName("es").setMaster("yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName).set("spark.driver.maxResultSize","10g")
  val sc = new SparkContext(conf_s)

  println("+++++++++++++++++++++++++++++++++++++       0:" + Integer.parseInt(args.apply(0)) + "_" + Integer.parseInt(args.apply(1)) + "_" + Integer.parseInt(args.apply(2)))

  val parts = 1000
  val data = new UConf(sc, parts)
  val all = data.getData

  val trees = Integer.parseInt(args.apply(0)) //50
  val grams = Integer.parseInt(args.apply(1))
  val grams2 = Integer.parseInt(args.apply(2))
  val fetures = Integer.parseInt(args.apply(3)) //10000

  val allSampled = all.sample(false, 0.1, 12345)

  allSampled.partitions.size //parseGramsTFIDFData
  val (trainingAll, testAll) = Transformer.splitRawDataByURL(allSampled)
  // val trainingData = Transformer.parseGramsTFIDFData(trainingAll, grams, grams2).repartition(parts)
  // val test = Transformer.parseGramsTFIDFData(testAll, grams, grams2).repartition(parts)
  val trainingData = Transformer.parseData(trainingAll, grams, 0).repartition(parts)
  val test = Transformer.parseData(testAll, grams, 0).repartition(parts)

  trainingData.partitions.size
  test.partitions.size
  //trainng idf
  val hashingTF = new HashingTF(500000)
  val tf: RDD[Vector] = hashingTF.transform(trainingData.map(l => l._2))
  val idf = (new IDF(minDocFreq = 10)).fit(tf)
  val idf_vector = idf.idf.toArray

  val tfidf_avg = Statistics.colStats(idf.transform(tf)).mean.toArray
  val selected_indices = Transformer.getTopTFIDFIndices(fetures, tfidf_avg)
  val idf_vector_filtered = Transformer.projectByIndices(idf_vector, selected_indices)

  val training_points = Transformer.data2points(trainingData, idf_vector_filtered, selected_indices, hashingTF).repartition(parts)
  val test_points = Transformer.data2points(test, idf_vector_filtered, selected_indices, hashingTF).repartition(parts)

  import org.apache.spark.mllib.tree.RandomForest
  // Train a RandomForest model.
  val treeStrategy = Strategy.defaultStrategy("Classification")
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val model = RandomForest.trainClassifier(training_points, treeStrategy, trees, featureSubsetStrategy, seed = 12345)

  // Evaluate model on test instances and compute test error
  val res = Transformer.labelAndPred(test_points, model)
  Utils.write2File("RFees_" + trees + "_grams_" + grams + "_grams2_" + grams2 + "_fetures_" + fetures + "_res_" + res, sc)
}