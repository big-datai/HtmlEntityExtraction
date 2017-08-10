package um.re.models

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import um.re.data.Url
import um.re.transform.{DFTransformer, Transformer}
import um.re.utils.UConf

object GBTDomainBySchema extends App {
  val conf_s = new SparkConf
  val sc = new SparkContext(conf_s)

  val data = new UConf(sc, 200)
  val all = data.getData
  val df = DFTransformer.rdd2DF(all, sc).cache
  val list = args(0).split(",").filter(s => !s.equals(""))

  for (d <- list) {

    val oneDomain = df.filter("domain = '" + d + "'").cache
    val urls = oneDomain.select("url").distinct.rdd.map { l => l.getString(0) }
    //val group = urls.groupBy(_.toString) //map{l=>(l.get(4),l)}.groupByKey

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val splits = urls.randomSplit(Array(0.7, 0.3))
    val (training, test) = (splits(0).map(l => Url(l)).toDF, splits(1).map(l => Url(l)).toDF)
    val dfTraining = oneDomain.join(training, oneDomain.col("url").equalTo(training("url")))
    val dfTest = oneDomain.join(test, oneDomain.col("url").equalTo(test("url")))
    dfTraining.show

    val hash = new org.apache.spark.mllib.feature.HashingTF(500000)
    val tf: RDD[Vector] = hash.transform(dfTraining.select("tokens").map(l => l.get(0).asInstanceOf[Seq[String]]))
    val idf = (new IDF(minDocFreq = 10)).fit(tf)
    val idf_vector = idf.idf.toArray
    val tfidf_avg = Statistics.colStats(idf.transform(tf)).mean.toArray
    val selected_indices = Transformer.getTopTFIDFIndices(100, tfidf_avg)
    val idf_vector_filtered = Transformer.projectByIndices(idf_vector, selected_indices)

    //TODO data2points
    /*    
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
*/
  }

}