package um.re.models

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.{ HashingTF, IDF }
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.stat.Statistics
import um.re.transform.Transformer
import um.re.utils.{ UConf }
import um.re.transform.DFTransformer

object GBTDomainBySchema extends App {
  val conf_s = new SparkConf
  val sc = new SparkContext(conf_s)

  val data = new UConf(sc, 200)
  val all = data.getData

  val df = DFTransformer.rdd2DF(all, sc)

  //TODO create list of domains that are relevant
  val list = args(0).split(",").filter(s => !s.equals(""))
  //$trees tp     fp    ...

  var domain2ScoreMap: Map[String, IndexedSeq[(Int, (Long, Long, Long, Long, Double, Double, Double, Double, Double))]] = Map.empty

  for (d <- list) {

    // filter domain group by url (url => Iterator.cadidates)
    val parsedDataPerURL = Transformer.parseDataPerURL(all).filter(l => l._2._4.equals(d)).groupBy(_._1)

    val splits = parsedDataPerURL.randomSplit(Array(0.7, 0.3))
    val (training, test) = (splits(0).flatMap(l => l._2), splits(1).flatMap(l => l._2))

    val hashingTF = new HashingTF(300000)
    val tf: RDD[Vector] = hashingTF.transform(training.map(l => l._2._2))
    val idf = (new IDF(minDocFreq = 10)).fit(tf)
    val idf_vector = idf.idf.toArray

    val tfidf_avg = Statistics.colStats(idf.transform(tf)).mean.toArray
    val selected_indices = Transformer.getTopTFIDFIndices(100, tfidf_avg)
    val idf_vector_filtered = Transformer.projectByIndices(idf_vector, selected_indices)

    val training_points = Transformer.data2pointsPerURL(training, idf_vector_filtered, selected_indices, hashingTF).map(p => p._2).repartition(10)
    val test_points = Transformer.data2pointsPerURL(test, idf_vector_filtered, selected_indices, hashingTF).repartition(10)

    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 30
    //boostingStrategy.treeStrategy.maxDepth = 5
    val model = GradientBoostedTrees.train(training_points, boostingStrategy)

    val subModels = Transformer.buildTreeSubModels(model)
    val scoresMap = subModels.map(m => Transformer.evaluateModel(Transformer.labelAndPredPerURL(m, test_points), m))
    domain2ScoreMap = domain2ScoreMap.updated(d, scoresMap)
    val domain2ScoreList = domain2ScoreMap.toList.map { l =>
      l._2.map { s => l._1 + " : " + s.toString }.mkString("\n")
    }
    sc.parallelize(domain2ScoreList, 1).saveAsTextFile("s3://pavlovout/dscores/" + d) // list on place i

    //TODO add function to choose candidates and evaluate on url level

    //TODO CHOOSE MODEL BY F
    val prec =
      // val F=2*(prec*recall)/(prec+recall)
      sc.parallelize(subModels, 1).saveAsObjectFile("/user/" + d)

  }

}