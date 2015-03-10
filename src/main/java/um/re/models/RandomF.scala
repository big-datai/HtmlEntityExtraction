package um.re.models

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.serializer.KryoSerializer

object RandomF extends App {
  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)
  // Load and parse the data file.
  val data =
    MLUtils.loadLibSVMFile(sc, "hdfs:///pavlovout/points")
  // Split data into training/test sets
  val splits = data.randomSplit(Array(0.7, 0.3))

  val (trainingData, test) = (splits(0), splits(1))
  val testData = test//sc.makeRDD(test.take(1000))
  trainingData.cache
  testData.cache
  // Train a RandomForest model.
  //val categoricalFeaturesInfo = Map[Int, Int]()  category_name number of values 2 {0/1} 
  val treeStrategy = Strategy.defaultStrategy("Classification")
  val numTrees = 3 // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val model = RandomForest.trainClassifier(trainingData,
    treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)

  // Evaluate model on test instances and compute test error
  val testSuc = testData.map { point =>
    val prediction = model.predict(point.features)
    if (point.label == prediction) 1.0 else 0.0
  }.mean()
  println("Test success = " + testSuc)
  println("Learned Random Forest:\n" + model.toDebugString)
}