package mllib.analytics

import scala.Array.canBuildFrom
import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo.Classification
import org.apache.spark.mllib.tree.impurity.Gini
import scala.util.control.Exception.allCatch

object DecisionTree_Dmitry {
  def main(str: Array[String]) {
    //Prepare data for the algorithm by splitting into label points
    def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined
    val conf = new SparkConf().setAppName("DecisionTree_Dmitry").setMaster("local[4]")
    /*
  //configure spark context
  val sparkHome = "/root/spark"
  val master = Source.fromFile("/root/spark-ec2/cluster-url").mkString.trim
  val masterHostname = Source.fromFile("/root/spark-ec2/masters").mkString.trim
  val jarFile = "target/mvnscala-1.0-SNAPSHOT.jar"
  val conf = new SparkConf().setAppName("DecisionTree_Dmitry").setMaster(master).setSparkHome(sparkHome).set("spark.executor.memory", "1g").setJars(Seq(jarFile))
 */ val sc = new SparkContext(conf)

    //configure S3 access 
    val awsAccessKey = "AKIAJAIFT2JLQZZU4SCA";
    val awsSecretKey = "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";
    //val awsCredentials = new AWSCredentials(awsAccessKey, awsSecretKey)

    // Load and parse the data decision tree ready file for test purpose.
    //val data = sc.textFile("C:\\Users\\dmitry.pavlov\\Desktop\\sample_tree_data.csv")
    val data = sc.textFile("C:\\Users\\dmitry.pavlov\\Desktop\\query_result2.csv")

    // Upload rtb data
    // val data1 = Source.fromFile("s3n://AKIAJAIFT2JLQZZU4SCA:n8OCRRHfShStgbKncg82EnakeCCzCpSAPYbT+7g7@pavlovP")
    //To communicate with S3, create a class that implements an S3Service. We will use the REST/HTTP implementation based on HttpClient, as this is the most robust implementation provided with JetS3t.
    //val s3Service = new RestS3Service(awsCredentials)
    //  val objects = s3Service.listObjects("pavlovP")

    val data2 = data.filter(line => line.r.pattern.matcher("[a-zA-Z]").matches)

    /*  
  // Print out each object's key and size.
  var sum = 0
  var count = 0
  for (o <- objects) {
    val aFile = o.getKey()
    val data2 = sc.textFile("s3n://AKIAJAIFT2JLQZZU4SCA:n8OCRRHfShStgbKncg82EnakeCCzCpSAPYbT+7g7@pavlovP/" + aFile)
    data2.map(x => println(x))
    val parsedData = data.map { line =>
      val parts = line.split('').map(_.toDouble)
      LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }
    println(" " + aFile)
    count = count + 1
  }
  println(count)
*/
    //Parsing data in SOH format
    /* for (x <- data) {
    println("x")
    println(x.split("\u0001") mkString)
  }*/
    /*
  //to parse SOH
  val csvFormated = data.map { line =>
    val parts = line.split("\u0001")
    (parts(0), parts.tail mkString (","))
  }

  for (x <- csvFormated) {
    //println("x")
    println(x._1 + "  ++++++++++++++++  " + x._2)
  }
*/

    val parsedData = data2.map { line =>
      val parts = line.split(",").map(_.toDouble)
      LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }

    for (line <- data2) {
      //val parts = line.split(",").filter(p => isDoubleNumber(p)).map(_.toDouble).mkString(" - ")
      //println(parts)
    }

    // Run training algorithm to build the model
    val maxDepth = 5
    val model = DecisionTree.train(parsedData, Classification, Gini, maxDepth)

    // Evaluate model on training examples and compute training error
    val labelAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    for (x <- labelAndPreds) {
      println(x._1 + " ------------------------------------ " + x._2)
    }

    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count
    println("Training Error = " + trainErr)

    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++  Regression and variance++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//

    // Run training algorithm to build the model
    /*
val model2 = DecisionTree.train(parsedData, Regression, Variance, maxDepth)

// Evaluate model on training examples and compute training error
val valuesAndPreds = parsedData.map { point =>
  val prediction = model2.predict(point.features)
  (point.label, prediction)
}
val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2)}

//println("training Mean Squared Error = " + MSE.)
*/
  }
}