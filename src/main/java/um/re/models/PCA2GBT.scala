package um.re.models


import org.apache.hadoop.io.{MapWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.elasticsearch.hadoop.mr.EsInputFormat
import um.re.utils.{EsUtils, Utils}

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.mapAsScalaMap

object PCA2GBT {

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val conf = new JobConf()
  conf.set("es.resource", EsUtils.ESINDEX)
  conf.set("es.nodes", EsUtils.ESIP)

  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val all = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString()) }.toMap) }
  //.repartition(600)
  val domains = all.map { l => Utils.getDomain(l._2.apply("url")) }.distinct(600).collect.zipWithIndex.toMap
  val parsedData = parseData(all)
  val splits = parsedData.randomSplit(Array(0.7, 0.3))
  val (trainingData, test) = (splits(0), splits(1))
  //trainng idf
  val hashingTF = new HashingTF(300000)
  val tf: RDD[Vector] = hashingTF.transform(trainingData.map(l => l._2))
  val idf = (new IDF(minDocFreq = 10)).fit(tf)
  val idf_vector = idf.idf.toArray
  val tfidf = idf.transform(tf)
  val tfidf_means = Statistics.colStats(tfidf).mean
  val tfidf_avg = tfidf_means.toArray

  /*def centeralize(rdd:RDD[Vector])={
    val means = tfidf_means
    rdd.map{ v=>
      val values = (v.toArray,means.toArray).zipped.map((d1,d2)=>d1-d2)
      Vectors.dense(values)
      
    }
  }*/
  val k = scala.math.min(1000, tfidf_avg.toList.filter(v => v != 0).size) //number of tdudf features
  val tfidf_avg_sorted = tfidf_avg.clone
  val top_k_value = tfidf_avg_sorted.takeRight(k)(0)
  java.util.Arrays.sort(tfidf_avg_sorted)
  val selected_indices = (for (i <- tfidf_avg.indices if tfidf_avg(i) >= top_k_value) yield i).toArray
  val idf_vector_filtered = selected_indices.map(i => idf_vector(i))
  val trainDateForPCA_p = reduce_features(trainingData.filter(l => l._1 == 1))
  val trainDateForPCA_n = reduce_features(trainingData.filter(l => l._1 == 0))
  val testDateForPCA_p = reduce_features(test.filter(l => l._1 == 1))
  val testDateForPCA_n = reduce_features(test.filter(l => l._1 == 0))
  val training_points = data_to_points(trainDateForPCA_p, 1) ++ data_to_points(trainDateForPCA_n, 0)
  val test_points = data_to_points(testDateForPCA_p, 1) ++ data_to_points(testDateForPCA_n, 0)
  val tr_pnts = training_points.repartition(192)
  val boostingStrategy =
    BoostingStrategy.defaultParams("Classification")
  val model =
    GradientBoostedTrees.train(tr_pnts /*training_points*/ , boostingStrategy)
  /*
  val numClasses = 2
  val categoricalFeaturesInfo = Map[Int, Int]()
  val numTrees = 3 // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val impurity = "variance"
  val maxDepth = 4
  val maxBins = 32

  val model = RandomForest.trainRegressor(training_points, categoricalFeaturesInfo,
    numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    // Evaluate model on test instances and compute test error
    */
  val algo = model.algo
  val trees = model.trees
  boostingStrategy.numIterations = 500 // Note: Use more in practice
  boostingStrategy.treeStrategy.maxDepth = 5
  //boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map(101->domains.size)
  //boostingStrategy.treeStrategy.maxBins = domains.size
  val treeW = model.treeWeights
  val numTrees = trees.length
  var scoresMap: Map[Int, (Long, Long, Long, Long, Double, Double, Double)] = Map.empty

  def parseData(raw: RDD[(String, Map[String, String])]) = {
    val domain_map = domains
    raw.map { l =>
      val before = Utils.tokenazer(l._2.apply("text_before"))
      val after = Utils.tokenazer(l._2.apply("text_after"))
      val domain = domain_map.apply(Utils.getDomain(l._2.apply("url")))
      val location = Integer.valueOf(l._2.apply("location")).toDouble
      val parts = before ++ after //, location)
    val parts_embedded = parts //.filter { w => (!w.isEmpty() && w.length > 3) }.map { w => w.toLowerCase }
      if ((l._2.apply("priceCandidate").contains(l._2.apply("price"))))
        (1, parts_embedded, location, domain)
      else
        (0, parts_embedded, location, domain)
    }.filter(l => l._2.length > 1 /*&& l._4==32*/)
  }

  def reduce_features(data: RDD[(Int, Seq[String], Double, Int)]) = {
    val idf_vals = idf_vector_filtered
    val tf_model = hashingTF
    val selected_ind_vals = selected_indices
    data.map { case (lable, txt, location, domain) =>
      val tf_vals_full = tf_model.transform(txt).toArray
      val tf_vals = selected_ind_vals.map(i => tf_vals_full(i))
      val tfidf_vals = (tf_vals, idf_vals).zipped.map((d1, d2) => d1 * d2)
      val features = tfidf_vals ++ Array(location, domain)
      val values = features.filter { l => l != 0 }
      val index = features.zipWithIndex.filter { l => l._1 != 0 }.map { l => l._2 }
      (Vectors.sparse(features.length, index, values))
    }
  }

  /*
  val dateForPCA = trainDateForPCA_p++trainDateForPCA_n
  val mat_train: RowMatrix = new RowMatrix(dateForPCA)
  val pc = mat_train.computePrincipalComponents(100)


  def data_to_points_PCA(data:RDD[Vector],label:Int) = {
    val pca_mat = pc
    new RowMatrix(data).multiply(pca_mat).rows.map(v=>
    	LabeledPoint(label,v))
    }
  */
  def data_to_points(data: RDD[Vector], label: Int) = {
    data.map(v =>
      LabeledPoint(label, v))
  }
  for (i <- List(10, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000)) {
    val model_i = new GradientBoostedTreesModel(algo, trees.take(i), treeW.take(i))

    def labelAndPred(input_points: RDD[LabeledPoint]) = {
      val local_model = model_i
      val labelAndPreds = input_points.map { point =>
        val prediction = local_model.predict(point.features)
        (point.label, prediction)
      }
      labelAndPreds
    }

    val labelAndPreds = labelAndPred(test_points)
    val tp = labelAndPreds.filter { case (l, p) => (l == 1) && (p == 1) }.count
    val tn = labelAndPreds.filter { case (l, p) => (l == 0) && (p == 0) }.count
    val fp = labelAndPreds.filter { case (l, p) => (l == 0) && (p == 1) }.count
    val fn = labelAndPreds.filter { case (l, p) => (l == 1) && (p == 0) }.count
    val sen = tp / (tp + fn).toDouble
    val spec = tn / (fp + tn).toDouble
    val prec = tp / (tp + fp).toDouble
    scoresMap = scoresMap.updated(i, (tp, tn, fp, fn, sen, spec, prec))
  }

  for (i <- 0 to scoresMap.size - 1)
    println("numTrees : " + i + " -- " + scoresMap.apply(i))


  //  println("tp : " + tp + ", tn : " + tn + ", fp : " + fp + ", fn : " + fn)
  //  println("sensitivity : " + sen + " specificity : " + spec + " precision : " + prec)

}