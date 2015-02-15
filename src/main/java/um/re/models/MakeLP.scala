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

object MakeLP extends App {

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val conf = new JobConf()
  conf.set("es.resource", "candidl/data")
  conf.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")
  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val all = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString()) }.toMap) }.repartition(100)
  //merge text before and after

  val parsedData = all.map { l =>
    val before = Utils.tokenazer(l._2.apply("text_before"))
    val after = Utils.tokenazer(l._2.apply("text_after"))
    val domain = Utils.getDomain(l._2.apply("url"))
    val location = l._2.apply("location")
    val parts = before ++ after ++ Array(domain) //, location) 
    val parts_embedded = parts //.filter { w => (!w.isEmpty() && w.length > 3) }.map { w => w.toLowerCase }
    if ((l._2.apply("priceCandidate").contains(l._2.apply("price"))))
      (1, parts_embedded)
    else
      (0, parts_embedded)
  }.filter(l => l._2.length > 1)
  parsedData.take(10).foreach(println)

  val docs = parsedData.map(l => l._2).flatMap(f => f)

  val splits = parsedData.randomSplit(Array(0.7, 0.3))
  val (trainingData, test) = (splits(0), splits(1))

  val documents_p: RDD[Seq[String]] = trainingData.filter(l => l._1 == 1).map(l => l._2)
  //POSITIVE
  val hashingTF = new HashingTF(50000)
  val tf: RDD[Vector] = hashingTF.transform(documents_p)

  import org.apache.spark.mllib.feature.IDF

  val idf = (new IDF(minDocFreq = 10)).fit(tf)
  val tfidf: RDD[Vector] = idf.transform(tf)
  //NEGATIVE
  val documents_n: RDD[Seq[String]] = trainingData.filter(l => l._1 == 0).map(l => l._2)

  val hashingTFn = new HashingTF(50000)
  val tfn: RDD[Vector] = hashingTFn.transform(documents_n)

  val idfn = (new IDF(minDocFreq = 10)).fit(tfn)
  val tfidfn: RDD[Vector] = idfn.transform(tfn)

  val positive = tfidf.map { l => LabeledPoint(1, l) }
  val negat = tfidfn.map { l => LabeledPoint(0, l) }
  //ALL TOGETHER
  val points = positive ++ negat
  //SAVE TO FILE ALL DATA
  MLUtils.saveAsLibSVMFile(points, "hdfs:///pavlovout/points")
  MLUtils.saveAsLibSVMFile(points, "s3://pavlovout/points")
  // val tf_ppoints = tf.map { l => LabeledPoint(1, l) } ++ tfn.map { l => LabeledPoint(0, l) }
  //SAVE TO FILE ALL DATA
  MLUtils.saveAsLibSVMFile(points, "hdfs:///pavlovout/points")
  test.saveAsTextFile("hdfs:///pavlovout/test_text")
  test.saveAsObjectFile("hdfs:///pavlovout/test_obj")
  val l = sc.textFile("hdfs:///pavlovout/test_text")
  val ll = sc.objectFile("hdfs:///pavlovout/test_obj")
  //test data
  val test_p = test.filter(l => l._1 == 1).map(l => l._2)
  val test_n = test.filter(l => l._1 == 0).map(l => l._2)

  val test2: RDD[Seq[String]] = test.map(l => l._2)
  val hashingTFtest = new HashingTF(50000)
  val tftest: RDD[Vector] = hashingTFtest.transform(test2)
  val idft = (new IDF(minDocFreq = 10)).fit(tftest)
  val tfidft: RDD[Vector] = idft.transform(tftest)
  
  //var conf:org.apache.hadoop.mapred.JobConf=null
  //var source:org.apache.spark.rdd.RDD[(Text, MapWritable)]=null
}