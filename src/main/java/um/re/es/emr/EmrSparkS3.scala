package um.re.es.emr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.serializer.KryoSerializer

object EmrSparkS3 {

  val conf_s = new SparkConf().setAppName("s3").set("master", "yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)
  
  val source = sc.textFile("s3://pavlovP/*.ready")
  val counts = source.flatMap { l => l.split(" ") }.map(word => (word, 1)).reduceByKey(_ + _)
  counts.count
  counts.saveAsTextFile("s3://pavlovout/15")

}