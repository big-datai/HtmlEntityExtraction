package mllib.analytics

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

object DataSamples {

  def main(args: Array[String]) {

    print("starting local spark")
    val conf = new SparkConf().setAppName("appnam").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("sample.data")
    println(textFile.count)

    // Create a dense vector (1.0, 0.0, 3.0).
    val dv = Vectors.dense(1.0, 0.0, 3.0)

    println(dv)

    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
    val sv1 = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    println(sv1)

    // Create a labeled point with a positive label and a dense feature vector.
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

    println(pos)
    
    val examples = MLUtils.loadLibSVMFile(sc, "sample_libsvm_data.txt")
    println(examples.first)
  }

}