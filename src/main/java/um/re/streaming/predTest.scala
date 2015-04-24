package um.re.streaming

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import um.re.transform.Transformer

object predTest extends App {

  val sc = new SparkContext()

  // Load and parse the data file.
  val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
  // Split the data into training and test sets (30% held out for testing)
  val splits = data.randomSplit(Array(0.7, 0.3))
  val (trainingData, testData) = (splits(0), splits(1))

  // Train a GradientBoostedTrees model.
  //  The defaultParams for Regression use SquaredError by default.
  val boostingStrategy = BoostingStrategy.defaultParams("Classification")
  boostingStrategy.numIterations = 30
  boostingStrategy.treeStrategy.maxDepth = 5
  //  Empty categoricalFeaturesInfo indicates all features are continuous.
  boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

  val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

  // Evaluate model on test instances and compute test error
  val labelsAndPredictions = testData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction, Transformer.confidenceGBT(model,point.features))
  }

  labelsAndPredictions.collect.foreach(println)

}