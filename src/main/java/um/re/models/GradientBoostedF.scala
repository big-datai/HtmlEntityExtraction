package um.re.models

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils

object GradientBoostedF extends App {
  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  // Load and parse the data file.
  val data =
    MLUtils.loadLibSVMFile(sc, "hdfs:///pavlovout/points")
  // Split data into training/test sets
  val splits = data.randomSplit(Array(0.7, 0.3))

  val (trainingData, test) = (splits(0), splits(1))
  val testData = sc.makeRDD(test.take(10000))
  trainingData.cache
  testData.cache
  // Train a GradientBoostedTrees model.
  val boostingStrategy =
    BoostingStrategy.defaultParams("Classification")
  boostingStrategy.numIterations = 3 // Note: Use more in practice
  val model =
    GradientBoostedTrees.train(trainingData, boostingStrategy)

  // Evaluate model on test instances and compute test error
  val testErr = testData.map { point =>
    val prediction = model.predict(point.features)
    println("real value :" + point.label + " prediction " + prediction)
    if (point.label == prediction) 1.0 else 0.0
  }.mean()
  
   val real_predic = testData.map { point =>
    val prediction = model.predict(point.features)
    if(point.label !=prediction)
    	(point.label, prediction) 
  }
  real_predic.take(10000).foreach(println)
  println("Test Precision = " + testErr)
  println("Learned GBT model:\n" + model.toDebugString)
}