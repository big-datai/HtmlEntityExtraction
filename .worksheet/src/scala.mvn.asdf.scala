package scala.mvn

object asdf {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(76); 
  println("Welcome to the Scala worksheet")}
  

import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.mllib.tree.impurity.Variance


object DecisionTree_Dmitry extends App {

  // val conf = new SparkConf().setAppName("DecisionTree_Dmitry").setMaster("local[3]")

  val sparkHome = "/root/spark"
  val master = Source.fromFile("/root/spark-ec2/cluster-url").mkString.trim
  val masterHostname = Source.fromFile("/root/spark-ec2/masters").mkString.trim
  val jarFile = "target/mvnscala-1.0-SNAPSHOT.jar"
  val conf = new SparkConf().setAppName("DecisionTree_Dmitry").setMaster(master).setSparkHome(sparkHome).set("spark.executor.memory", "1g").setJars(Seq(jarFile))
  val sc = new SparkContext(conf)

  // Load and parse the data file
  val data = sc.textFile("sample_tree_data.csv")
  val parsedData = data.map { line =>
    val parts = line.split(',').map(_.toDouble)
    LabeledPoint(parts(0), Vectors.dense(parts.tail))
  }

  // Run training algorithm to build the model
  val maxDepth = 5
  val model = DecisionTree.train(parsedData, Classification, Gini, maxDepth)

  // Evaluate model on training examples and compute training error
  val labelAndPreds = parsedData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }
  val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count
  println("Training Error = " + trainErr)

 //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++  Regression and variance++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
  
  // Run training algorithm to build the model

val model2 = DecisionTree.train(parsedData, Regression, Variance, maxDepth)

// Evaluate model on training examples and compute training error
val valuesAndPreds = parsedData.map { point =>
  val prediction = model2.predict(point.features)
  (point.label, prediction)
}
val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2)}.mean()

println("training Mean Squared Error = " + MSE)


}
}
