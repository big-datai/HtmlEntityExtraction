package um.re.models

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import um.re.transform.Transformer
import um.re.utils.UConf

object GBTLocation extends App {
  val conf_s = new SparkConf().setAppName("es").setMaster("yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val data = new UConf(sc, 200)
  val all = data.getData
  val parsedData = Transformer.parseData(all)

  val d = Transformer.dataSample(0.1, parsedData)
  val splits = d.randomSplit(Array(0.7, 0.3))
  val (trainingData, test) = (splits(0), splits(1))

  //trainng idf
  val hashingTF = new HashingTF(50000)
  val tf: RDD[Vector] = hashingTF.transform(trainingData.map(l => l._2))
  val idf = (new IDF(minDocFreq = 10)).fit(tf)
  //val tfidf = idf.transform(tf)
  val idf_vector = idf.idf.toArray

  val training_points = Transformer.data2points(trainingData, idf_vector, hashingTF)
  val test_points = Transformer.data2points(test, idf_vector, hashingTF)

  val boostingStrategy = BoostingStrategy.defaultParams("Classification")
  boostingStrategy.numIterations = 3 // Note: Use more in practice
  //boostingStrategy.treeStrategy.maxDepth = 2
  val model = GradientBoostedTrees.train(training_points, boostingStrategy)
  val res = Transformer.labelAndPred(test_points, model)

}