package um.re.models

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.concurrent.TrieMap
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.SparkContext.intToIntWritable
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.serializer.KryoSerializer
import um.re.utils.Utils
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.elasticsearch.spark.rdd.EsSpark
import um.re.es.emr.URegistrator
import scala.collection.immutable.TreeMap

object TFIDF2 extends App {

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)
  //Load data from ES

  val conf = new JobConf() //sc.hadoopConfiguration
  conf.set("es.resource", "candidl/data")
  conf.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")
  //val source = EsSpark.esRDD(sc,"ec2-54-167-216-26.compute-1.amazonaws.com")
  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable]).repartition(100).cache

  new URegistrator

  //prepare mapping and idf
  //Calculate collection tf for idf
  val c_tf = source.flatMap { l => Utils.tokenazer(l._2.toString) }.map(word => (word, 1)).reduceByKey(_ + _)
  val c_dist = c_tf.map { case (k, v) => k }.distinct.toArray
  //Creating a dictionary with a unique number for each word 
  val string2number: TrieMap[String, Long] = new TrieMap
  c_dist.foreach { l => string2number.putIfAbsent(l, 1) }
  var c = 1;
  val mappingTerms2Int = string2number.map { l =>
    c = c + 1
    (l._1, c)
  }
  val mapping = sc.makeRDD(mappingTerms2Int.toSeq).cache
  mapping.count

  val c_tfNumberBumber: TrieMap[Long, Int] = new TrieMap[Long, Int]
  c_tf.toArray.map { p => c_tfNumberBumber.putIfAbsent(mappingTerms2Int.apply(p._1).get, p._2) }

  val all = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString()) }.toMap) }
  //merge text before and after
  val cand = all.map { l => (l._1, l._2.filterNot { case (k, v) => (k.contains("pattern")) }) }

  val parsedData = cand.map { l =>
    val before = Utils.tokenazer(l._2.apply("text_before")).toArray
    val after = Utils.tokenazer(l._2.apply("text_after")).toArray
    val parts = after ++ before
    if ((l._2.get("priceCandidate").get.toString.contains(l._2.get("price").get.toString)))
      (1, parts)
    else
      (0, parts)
  }.filter(l => l._2.length > 1).cache

  //parsedData.saveAsTextFile("hdfs:///users/temp")

  //val j=parsedData.join(1,mapping)

  val points = parsedData.map { l =>
    val size = l._2.size
    val d_tf: TrieMap[String, Long] = new TrieMap

    var counter = 1;
    l._2.foreach { ll =>
      d_tf.putIfAbsent(ll, counter)
      counter = counter + 1
    }
    val loc_idf = d_tf.map { v => (mapping.filter { w => (w._1.equals(v)) }.first._2, v._2) }
    val locations = loc_idf.map { v => v._1 }.toArray
    val weights = loc_idf.map { v => v._2.toDouble }.toArray
    LabeledPoint(l._1, Vectors.sparse(size, locations, weights))
  }

  val points2 = parsedData.toArray.map { l =>
    LabeledPoint(l._1, Vectors.sparse(l._2.length, l._2.map { v => (mapping.filter { w => (w._1.equals(v)) }.first._2, 1.0) }.map { v => v._1 }.toArray,
      l._2.map { v => (mapping.filter { w => (w._1.equals(v)) }.first._2, 1.0) }.map { v => v._2 }.toArray))
  }

  val splits = points.randomSplit(Array(0.7, 0.3))
  val (trainingData, testData) = (splits(0), splits(1))

  // Train a RandomForest model.
  val treeStrategy = Strategy.defaultStrategy("Classification")
  val numTrees = 2 // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val model = RandomForest.trainClassifier(trainingData,
    treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)

  // Evaluate model on test instances and compute test error
  val testErr = testData.map { point =>
    val prediction = model.predict(point.features)
    if (point.label == prediction) 1.0 else 0.0
  }.mean()
  println("Test Error = " + testErr)
  println("Learned Random Forest:\n" + model.toDebugString)

}