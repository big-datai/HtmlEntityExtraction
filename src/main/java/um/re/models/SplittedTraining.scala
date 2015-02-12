package um.re.models

import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
//import org.elasticsearch.hadoop.mr.EsInputFormat[org.apache.hadoop.io.Text,org.apache.hadoop.io.{EsInputFormat => MapWritable]}
import scala.collection.JavaConversions.mapAsScalaMap
import scala.reflect.runtime.universe
import um.re.utils.Utils

object SplittedTraining extends App {

  import scala.Array.canBuildFrom
  import scala.collection.JavaConversions.mapAsScalaMap
  import org.apache.hadoop.io.MapWritable
  import org.apache.hadoop.io.Text
  import org.apache.hadoop.mapred.JobConf
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.ml.Pipeline
  import org.apache.spark.ml.feature.HashingTF
  import org.apache.spark.ml.feature.Tokenizer
  import org.apache.spark.mllib.linalg.Vector
  import org.apache.spark.rdd.RDD
  import org.apache.spark.serializer.KryoSerializer
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.SQLContext
  import org.elasticsearch.hadoop.mr.EsInputFormat
  import um.re.utils.Utils
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
  import org.apache.spark.mllib.tree.RandomForest
  import org.apache.spark.mllib.tree.configuration.Strategy
  import org.apache.spark.mllib.util.MLUtils
  import org.apache.spark.serializer.KryoSerializer
  import org.apache.spark.mllib.util.MLUtils
  import org.apache.spark.mllib.tree.configuration.BoostingStrategy
  import org.apache.spark.mllib.tree.GradientBoostedTrees
  import org.apache.spark.mllib.regression.LabeledPoint

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val sqlContext = new SQLContext(sc)
  import sqlContext._

  case class LabeledDocument(label: Int, text: String, d: String)
  val conf = new JobConf()
  conf.set("es.resource", "candidl/data")
  conf.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")
  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val all = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString()) }.toMap) }.repartition(200)
  //merge text before and after

  val data = all.map { l =>
    val before = Utils.textOnly(l._2.apply("text_before"))
    val after = Utils.textOnly(l._2.apply("text_after"))
    val domain = Utils.getDomain(l._2.apply("url"))
    val location = l._2.apply("location")
    val parts = before + after
    val parts_embedded = parts //.filter { w => (!w.isEmpty() && w.length > 3) }.map { w => w.toLowerCase }
    if ((l._2.get("priceCandidate").get.toString.contains(l._2.get("price").get.toString)))
      LabeledDocument(1, parts_embedded, domain)
    else
      LabeledDocument(0, parts_embedded, domain)
  }
  val splits = data.randomSplit(Array(0.7, 0.3))
  val (training, test) = (splits(0), splits(1))

  //TO LABEL POINTS
  // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
  val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
  val hashingTF = new HashingTF().setNumFeatures(50000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
  val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF))
  val t1 = pipeline.fit(training).transform(training).select('label, 'features)
  val ftraininf = t1.map {
    case Row(label: Int, features: Vector) =>
      LabeledPoint(label, features)
  }
  val t2 = pipeline.fit(test).transform(test).select('label, 'features)
  val ftest = t2.map {
    case Row(label: Int, features: Vector) =>
      LabeledPoint(label, features)
  }

  
  MLUtils.saveAsLibSVMFile(ftraininf, "hdfs:///pavlovout/pointsALL")
MLUtils.saveAsLibSVMFile(ftest, "hdfs:///pavlovout/testALL")

  val boostingStrategy =
    BoostingStrategy.defaultParams("Classification")
  boostingStrategy.numIterations = 3 // Note: Use more in practice
  val model =
    GradientBoostedTrees.train(ftraininf, boostingStrategy)

    
    
  // Evaluate model on test instances and compute test error
  val labelAndPreds = ftest.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }

  val testSuccess = labelAndPreds.map { point =>
    if (point._1 == point._2) 1.0 else 0.0
  }.mean()

  val real_predic = labelAndPreds.map { point =>
    if (point._1 != point._2 && point._1 == 0.0) {
      (point._1, point._2)
    } else
      null
  } filter { l => l != null }

  real_predic.take(10000).foreach(println)

  println("Precision = " + testSuccess)
  println("Learned GBT model:\n" + model.toDebugString)

}
