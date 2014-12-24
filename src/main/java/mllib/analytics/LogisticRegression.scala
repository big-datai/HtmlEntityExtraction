
package mllib.analytics

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import scala.Array.canBuildFrom

object LogisticRegressionExample extends App{

		// Load and parse the data file
		val conf = new SparkConf().setAppName("logistic").setMaster("local[4]").set("spark.executor.memory", "10g")
		val sc = new SparkContext(conf)
		val data =sc.textFile("sample_tree_data.csv")
		val parsedData = data.map { line =>
		  val parts = line.split(',')
		  LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
		}
		//LabeledPoint(parts(0).toDouble, parts.tail.map(x => x.toDouble).toArray)
		

		// Run training algorithm to build the model
		val numIterations = 20
		val model = LogisticRegressionWithSGD.train(parsedData, numIterations)

		// Evaluate model on training examples and compute training error
		val labelAndPreds = parsedData.map { point =>
		  val prediction = model.predict(point.features)
		  (point.label, prediction)
		}
		//labelAndPreds.foreach(println)
		
		val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count
		val n1 = labelAndPreds.filter(r => (r._1==1)&&(r._2==1)).count.toDouble
		val n2 = labelAndPreds.filter(r => (r._1==0)&&(r._2==0)).count.toDouble
		val d1 = labelAndPreds.filter(r => (r._1==1)).count.toDouble
		val d2 = labelAndPreds.filter(r => (r._1==0)).count.toDouble
		val sensitivity = n1/d1
		val specificity = n2/d2

		println("\nTraining Error = " + trainErr)
		println("Sensitivity = " + sensitivity)
		println("Specificity = " + specificity)
		println();
}