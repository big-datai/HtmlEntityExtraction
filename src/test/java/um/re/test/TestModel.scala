package um.re.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
import um.re.utils.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.hadoop.io.MapWritable
import org.apache.spark.SparkConf
import org.apache.hadoop.mapred.JobConf
import org.elasticsearch.hadoop.mr.EsInputFormat
import scala.Array.canBuildFrom
import scala.collection.JavaConversions.mapAsScalaMap
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.serializer.KryoSerializer
import um.re.utils.Utils
import org.elasticsearch.hadoop.mr.EsInputFormat
import scala.collection.concurrent.TrieMap
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.SVMWithSGD


object TestModel extends App {
  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val cand = sc.textFile("/cand")

  val sp = cand.filter { l => l.contains("\"text_") }

  val t = sp.toArray.mkString(" ")
  val splited = t.replaceAll("text_after", "").split("text_before").drop(1)

  val parsedData = sc.makeRDD(splited)

  val parsedData2 = parsedData.map { l => Utils.tokenazer(l) }
  parsedData2.take(1)
  
  val documents_p: RDD[Seq[String]] = parsedData2
  //POSITIVE
  val hashingTF = new HashingTF(50000)
  val tf: RDD[Vector] = hashingTF.transform(documents_p)

  import org.apache.spark.mllib.feature.IDF

  val idf = (new IDF(minDocFreq = 10)).fit(tf)
  val tfidf: RDD[Vector] = idf.transform(tf)
  val testData = tfidf.map { l => LabeledPoint(1, l) }
  /*  
    val labelAndPreds = testData.map { point =>
    model.predict(point.features)    
  }
labelAndPreds.collect.foreach(println)
 */
}