package mllib.analytics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object S3cript {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("analytics") //.setMaster("local")
    val sc = new SparkContext(conf)
    val files = sc.textFile("s3://pavlov-ml/19*.txt")
    val dictionary = files.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    
    dictionary.count

    val raw_us_sgiggle = files.filter(f => f.contains("US\",\"[{")).map(l => l.dropWhile(c => !c.equals(',')).replaceAll("[{}()\\[\\]\"]", "").
      replaceAll("install_time:\\d*", "").replaceAll("\\d\\d:\\d\\d:\\d\\d", "").replaceAll("\\d\\d\\d\\d-\\d\\d-\\d\\d", "")
      .replaceAll("package_name:", "").replaceAll(" ", ",").replaceAll(",+", ",")
      .dropRight(1).drop(1)).cache

   // counts.cache

    //val sortedList = reducedList.map(x => (x._2, x._1)).sortByKey(false).take(50)

  }
}