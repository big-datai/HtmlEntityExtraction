package um.re.models

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
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import um.re.utils.EsUtils
import um.re.utils.UConf
import um.re.transform.Transformer

object GBTLocationFeatureSelection extends App{

  val conf = new SparkConf().setAppName("es").set("master", "yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf)

  val data = new UConf(sc, 1000)
  val all = data.getData
  val parsedData = Transformer.parseData(all)

  val d = Transformer.dataSample(0.1, parsedData)
  val splits = d.randomSplit(Array(0.7, 0.3))
  val (trainingData, test) = (splits(0), splits(1))

  //trainng idf
  val hashingTF = new HashingTF(300000)
  val tf: RDD[Vector] = hashingTF.transform(trainingData.map(l => l._2))
  val idf = (new IDF(minDocFreq = 10)).fit(tf)
  val idf_vector = idf.idf.toArray

  val tfidf_avg = Statistics.colStats(idf.transform(tf)).mean.toArray
  val selected_indices = Transformer.getTopTFIDFIndices(100,tfidf_avg)
  val idf_vector_filtered = Transformer.projectByIndices(idf_vector, selected_indices) 
  
  val training_points = Transformer.data2points(trainingData, idf_vector_filtered, hashingTF)
  val test_points = Transformer.data2points(test, idf_vector_filtered, hashingTF)

  val boostingStrategy =BoostingStrategy.defaultParams("Classification")
  boostingStrategy.numIterations = 2
  boostingStrategy.treeStrategy.maxDepth = 5
  val model =GradientBoostedTrees.train(training_points, boostingStrategy)

  // Evaluate model on test instances and compute test error
  val res = Transformer.labelAndPred(test_points, model)
  res.saveAsTextFile("/user/gbt")
}