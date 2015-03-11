package um.re.models

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.mapAsScalaMap
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
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
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.ml.feature.HashingTF
import um.re.utils.EsUtils

object MakeLPPipe extends App {

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val sqlContext = new SQLContext(sc)
  import sqlContext._

  case class LabeledDocument(label: Int, text: String, d: String)
  val conf = new JobConf()
  conf.set("es.resource", EsUtils.ESINDEX)
  conf.set("es.nodes", EsUtils.ESIP)
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

  val tpos = training.filter { l => l.label == 1 }
  val tneg = training.filter { l => l.label == 0 }
  //TO LABEL POINTS
  // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
  val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
  val hashingTF = new HashingTF().setNumFeatures(50000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")

  val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF))

  val t1 = pipeline.fit(tpos).transform(tpos).select('label, 'features)
  val ftpos = t1.map {
    case Row(label: Int, features: Vector) =>
      features //LabeledPoint(label, features)
  }
  val k = ftpos.take(1).head

  val idf = (new IDF(minDocFreq = 10)).fit(ftpos)
  val tfidf: RDD[Vector] = idf.transform(ftpos)

  val t2 = pipeline.fit(tneg).transform(tneg).select('label, 'features)
  val ftneg = t2.map {
    case Row(label: Int, features: Vector) =>
      features //LabeledPoint(label, features)
  }
  val idfn = (new IDF(minDocFreq = 10)).fit(ftneg)
  val tfidfn: RDD[Vector] = idf.transform(ftneg)

  val t3 = pipeline.fit(test).transform(test).select('label, 'features)
  var ftest = t3.map {
    case Row(label: Int, features: Vector) =>
      LabeledPoint(label, features)
  }
  var ftestf = t3.map {
    case Row(label: Int, features: Vector) =>
      LabeledPoint(label, features)
  }

  //  val idft = (new IDF(minDocFreq = 10)).fit(ftestf)
  // val tfidft: RDD[Vector] = idf.transform(ftestf)

  var fintest = ftest.map { lp =>
    val v1 = lp.features.toString
    val start = v1.indexOf("[")
    val end = v1.indexOf("]")
    val w1 = v1.substring(start, end)
    if (ftestf != null) {
      (w1, lp)
    } //.filter { l => l != null }.count
    else
      null
  }.filter { l => l != null }

   ftest = MLUtils.loadLibSVMFile(sc, "hdfs:///pavlovout/testLabel")
   ftestf = MLUtils.loadLibSVMFile(sc, "hdfs:///pavlovout/testNotLabel")

  val res = ftestf.map { lp =>
    val v1 = lp.features.toString
    val start = v1.indexOf("[")
    val end = v1.indexOf("]")
    val w1 = v1.substring(start, end)
    if (ftestf != null) {
      (w1, lp)
    } //.filter { l => l != null }.count
    else
      null
  } filter { l => l != null }

  val resJoin = fintest.join(fintest)

  val test1 = resJoin.map {
    case Row(l1: LabeledPoint, l2: LabeledPoint) => (l1.label, l2.features)
  }.take(1)

  val lp = ftest.take(1).head
  val v2 = ftestf.take(1).head.features.toString

  val ftraininf = ftpos.map { l => LabeledPoint(1, l) } ++ ftneg.map { l => LabeledPoint(0, l) }

  MLUtils.saveAsLibSVMFile(ftraininf, "hdfs:///pavlovout/train")
  MLUtils.saveAsLibSVMFile(ftest, "hdfs:///pavlovout/testLabel")
  //MLUtils.saveAsLibSVMFile(ftestf.map { l => LabeledPoint(0, l) }, "hdfs:///pavlovout/testNotLabel")

  //laod data

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