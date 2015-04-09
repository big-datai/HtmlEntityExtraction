package um.re.domain.models

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.mllib.feature.{ HashingTF, IDF }
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import um.re.transform.Transformer
import um.re.utils.{ UConf }
import um.re.utils.Utils
import scala.collection.parallel.ForkJoinTaskSupport
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.HashPartitioner

object GBTDomainSuperPar extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)

  try {
	//TODO we should stick with this repartition or maybe use the hdfs default blocks  
    val parts = Integer.valueOf(args.apply(0))
    val data = new UConf(sc, parts)
    val all = data.getDataFS()
    // dMap as broadcast variable
    val dMap = sc.broadcast(sc.textFile((Utils.S3STORAGE + Utils.DMODELS + "part-00000"), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1))).toMap)
    //TODO either repartition by domain or don't repartition, the data were partitioned in UConf
    val partByDomain = new HashPartitioner(parts)
    val parsed = Transformer.parseDataPerURL(all).map { l => (l._2._4, l) }.partitionBy(partByDomain).mapPartitions({ p => p.map(_._2) }, true).cache

    // val dlist=sc.textFile((Utils.S3STORAGE + Utils.DMODELS + "dlist"), 1)
    //dlist.saveAsTextFile((Utils.S3STORAGE + Utils.DMODELS + "part-00000"), classOf[GzipCodec])
    val list = sc.textFile("/domains.list").flatMap { l => l.split(",").filter(s => !s.equals("")).filter(dMap.value.keySet.contains(_)) }.filter(s => !s.equals("")).toArray().toList
    val parList = list.par
    //val r = scala.util.Random

    for (d <- parList) {
      try {

        // parList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(1000))
        //Thread sleep r.nextInt(400000)
    	  sc.parallelize(Seq(""), 1).saveAsTextFile("/temp/list/" + dMap.value.apply(d) + System.currentTimeMillis().toString().replace(" ", "_"))
        val partForDomain = 10
        
        //TODO again none needed repartition before filter , post filter better group by key(url) and coalesce  
        //val parsedDataPerURL = parsed.repartition(300).filter(l => l._2._4.equals(d)).groupBy(_._1).repartition(10)
        val parsedDataPerURL = parsed.filter(l => l._2._4.equals(d)).coalesce(partForDomain).groupBy(_._1)
        val splits = parsedDataPerURL.randomSplit(Array(0.7, 0.3))
        val (training, test) = (splits(0).flatMap(l => l._2), splits(1).flatMap(l => l._2))
        val hashingTF = new HashingTF(1000)
        //TODO maybe cache tf
        val tf: RDD[Vector] = hashingTF.transform(training.map(l => l._2._2))
        val idf = (new IDF(minDocFreq = 5)).fit(tf)
        val idf_vector = idf.idf.toArray
        val tfidf_avg = Statistics.colStats(idf.transform(tf)).mean.toArray
        val selected_indices = Transformer.getTopTFIDFIndices(100, tfidf_avg)
        val idf_vector_filtered = Transformer.projectByIndices(idf_vector, selected_indices)

        //TODO cache points , coalesce instead of repartition if didn't repartition earlier otherwise don't repartition
        val training_points = Transformer.data2pointsPerURL(training, idf_vector_filtered, selected_indices, hashingTF).map(p => p._2).cache
        val test_points = Transformer.data2pointsPerURL(test, idf_vector_filtered, selected_indices, hashingTF)

        val boostingStrategy = BoostingStrategy.defaultParams("Classification")
        boostingStrategy.numIterations = 30
        boostingStrategy.treeStrategy.maxDepth = 5
        val model = GradientBoostedTrees.train(training_points, boostingStrategy)

        val res = Transformer.evaluateModel(Transformer.labelAndPredPerURL(model, test_points), model)
        val selectedModel = model
        val selectedScore = res

        val scoreString = d + selectedScore.toString
        try {
          sc.parallelize(Seq(scoreString), 1).saveAsTextFile(Utils.HDFSSTORAGE + "/temp" + Utils.DSCORES + dMap.value.apply(d) + System.currentTimeMillis().toString().replace(" ", "_")) // list on place i
          sc.parallelize(Seq(selectedModel),1).saveAsObjectFile(Utils.HDFSSTORAGE + "/temp" + Utils.DMODELS + dMap.value.apply(d) + System.currentTimeMillis().toString().replace(" ", "_"))
          training_points.unpersist(false)
          //S3 STORAGE
          //sc.parallelize(Seq(scoreString), 1).saveAsTextFile(Utils.S3STORAGE + Utils.DSCORES + dMap.value.apply(d), classOf[GzipCodec]) 
          // sc.parallelize(Seq(selectedModel)).saveAsObjectFile(Utils.S3STORAGE + Utils.DMODELS + dMap.value.apply(d))
        } catch {
          case _: Throwable => sc.parallelize(Seq("failed on writing the models"), 1).saveAsTextFile(Utils.HDFSSTORAGE + Utils.DSCORES + "Fails/" + dMap.value.apply(d) + System.currentTimeMillis().toString().replace(" ", "_"))
        }

        //TODO add function to choose candidates and evaluate on url level
        //TODO CHOOSE MODEL BY F
      } catch {
        case e: Throwable =>
          val errMsg = "model:  " + d + " " + e.getLocalizedMessage() + e.getMessage() + "try failed inside of for a big error"
          sc.parallelize(List(errMsg), 1).saveAsTextFile(Utils.HDFSSTORAGE + "/temp" + Utils.DMODELS + "log/" + errMsg + d + System.currentTimeMillis().toString().replace(" ", "_"))
      }
    }
  } catch {
    case e: Throwable =>
      val errMsg = e.getLocalizedMessage() + e.getMessage()
      sc.parallelize(List(errMsg + "program failed gloabal error"), 1).saveAsTextFile(Utils.HDFSSTORAGE + "/temp" + Utils.DMODELS + "log/" + errMsg + System.currentTimeMillis().toString().replace(" ", "_"))
  }
}