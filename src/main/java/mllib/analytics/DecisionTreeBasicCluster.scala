package mllib.analytics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext._
import scala.io.Source
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo.Classification
import org.apache.spark.mllib.tree.impurity.Gini

object DecisionTreeBasicCluster {

  def main(args: Array[String]) {

    // val conf = new SparkConf().setAppName("appnam").setMaster("local[3]")
    val sparkHome = "/root/spark"
    val master = Source.fromFile("/root/spark-ec2/cluster-url").mkString.trim
    val masterHostname = Source.fromFile("/root/spark-ec2/masters").mkString.trim
    val jarFile = "target/mvnscala-1.0-SNAPSHOT.jar"
    val conf = new SparkConf().setAppName("LinearRegression").setMaster(master).setSparkHome(sparkHome).set("spark.executor.memory", "1g").setJars(Seq(jarFile))
    val sc = new SparkContext(conf)

    // Load and parse the data
   // FileInputFormat.setInputPaths(conf, new Path("file://path of the In Folder on your File system "));
    val path="hdfs://"+masterHostname+ ":9000"+"/user/root/mvnscala/lpsa.data"
    println(path)
    val data = sc.textFile(path)
    
    
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

  println("loaded data " + data.count + "       ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

  // Run training algorithm to build the model
  val maxDepth = 5
  val model = DecisionTree.train(parsedData, Classification, Gini, maxDepth)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    
    val MSE = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.mean()
    println("training Mean Squared Error = " + MSE)
    sc.stop();
  }
}