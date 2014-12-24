package mllib.analytics

import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.optimization.L1Updater
import scala.io.Source

object BinarySVMWithSGD {

  //Stochastic Gradient Descent SGD
  def main(args: Array[String]) {

    val sparkHome = "/root/spark"
    val master = Source.fromFile("/root/spark-ec2/cluster-url").mkString.trim
    val masterHostname = Source.fromFile("/root/spark-ec2/masters").mkString.trim
    val jarFile = "target/mvnscala-1.0-SNAPSHOT.jar"
    val conf = new SparkConf().setAppName("BinarySVMWithSGD").setMaster(master).setSparkHome(sparkHome).set("spark.executor.memory", "1g").setJars(Seq(jarFile))
    val sc = new SparkContext(conf)

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    /*  Customize  
    val svmAlg = new SVMWithSGD()
    svmAlg.optimizer.
      setNumIterations(200).
      setRegParam(0.1).
      setUpdater(new L1Updater)
    val modelL1 = svmAlg.run(training)
*/
    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set. 
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

  }
}