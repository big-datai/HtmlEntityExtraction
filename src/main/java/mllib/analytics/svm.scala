package mllib.analytics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

object svm {

  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("appnam").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("sample.data")


    // Load and parse the data file
    val data = sc.textFile("sample_svm_data.txt")
    val parsedData = data.map { line =>
      val parts = line.split(' ')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(x => x.toDouble).toArray))
    }

    // Run training algorithm to build the model
    val numIterations = 20
    val model = SVMWithSGD.train(parsedData, numIterations)

    // Evaluate model on training examples and compute training error
    val labelAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    for(x<-labelAndPreds)
     println(x._1+" ---------------------- "+ x._2)
     
    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count
    println("Training Error = " + trainErr)
  }
}