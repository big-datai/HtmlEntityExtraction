package um.re.models

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import um.re.transform.Transformer
import um.re.utils.UConf

object GBTLocationFeatureSelection extends App {

  val conf = new SparkConf().setAppName("es").set("master", "yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf)

  val data = new UConf(sc, 1000)
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

  val tfidf_avg = Statistics.colStats(idf.transform(tf)).mean.toArray
  val selected_indices = Transformer.getTopTFIDFIndices(100, tfidf_avg)
  val idf_vector_filtered = Transformer.projectByIndices(idf_vector, selected_indices)

  val training_points = Transformer.data2points(trainingData, idf_vector_filtered, selected_indices, hashingTF)
  val test_points = Transformer.data2points(test, idf_vector_filtered, selected_indices, hashingTF)

  val boostingStrategy = BoostingStrategy.defaultParams("Classification")
  boostingStrategy.numIterations = 3
  boostingStrategy.treeStrategy.maxDepth = 5 ///4-8
  val model = GradientBoostedTrees.train(training_points, boostingStrategy)

  // Evaluate model on test instances and compute test error
  val res = Transformer.labelAndPred(test_points, model)
  res.saveAsTextFile("/user/gbt")
}