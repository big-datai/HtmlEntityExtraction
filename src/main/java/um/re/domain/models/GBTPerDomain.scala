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

object GBTPerDomain extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)
  try {

    val data = new UConf(sc, 500)
    val all = data.getData

    //val list = List("richtonemusic.co.uk","wholesalesupplements.shop.rakuten.com","shop.everythingbuttheweddingdress.com","DiscountCleaningProducts.com","yesss.co.uk","idsecurityonline.com","janitorialequipmentsupply.com","sanddollarlifestyles.com","protoolsdirect.co.uk","educationalinsights.com","faucet-warehouse.com","rexart.com","chronostore.com","racks-for-all.shop.rakuten.com","musicdirect.com","budgetpackaging.com","americanblinds.com","overthehill.com","thesupplementstore.co.uk","intheholegolf.com","alldesignerglasses.com","nitetimetoys.com","instrumentalley.com","ergonomic-chairs.officechairs.com","piratescave.co.uk")
    //val list = List("fawnandforest.com","parrotshopping.com")

    //list of domains 
    //TODO create list of domains that are relevant

    val dMap = sc.textFile((Utils.S3STORAGE + Utils.DMODELS + "dlist2"), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1))).toMap
    val parsed = Transformer.parseDataPerURL(all).cache

    val list = args(0).split(",").filter(s => !s.equals("")).filter(dMap.keySet.contains(_))

    sc.parallelize(Seq("",""), 1).saveAsTextFile("/dima/list/" + list.apply(0)+System.currentTimeMillis().toString().replace(" ", "_"))

    for (d <- list) {
      try {
        // filter domain group by url (url => Iterator.cadidates)
        val parsedDataPerURL = parsed.filter(l => l._2._4.equals(d)).groupBy(_._1)

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
        boostingStrategy.treeStrategy.maxDepth = 5
        val model = GradientBoostedTrees.train(training_points, boostingStrategy)

        val subModels = Transformer.buildTreeSubModels(model)
        val scoresMap = subModels.map(m => Transformer.evaluateModel(Transformer.labelAndPredPerURL(m, test_points), m))

        val bestRes = scoresMap.toList.map {
          case (k, v) =>
            val f = 2 * v._5 * v._7 / (v._5 + v._7)
            (f + v._9, k)
        }.sorted.reverse.apply(0)._2

        val selectedModel = subModels.apply(bestRes - 1)
        val selectedScore = scoresMap.filter(_._1 == bestRes)

        val scoreString = selectedScore.map { l =>
          d + " : " + l.toString
        }
        try {
          sc.parallelize(scoreString, 1).saveAsTextFile(Utils.HDFSSTORAGE + Utils.DSCORES + dMap.apply(d)+System.currentTimeMillis().toString().replace(" ", "_")) // list on place i
          selectedModel.save(sc, Utils.HDFSSTORAGE + Utils.DMODELS + dMap.apply(d)+System.currentTimeMillis().toString().replace(" ", "_"))
          // sc.parallelize(scoreString, 1).saveAsTextFile(Utils.S3STORAGE + Utils.DSCORES + dMap.apply(d)) // list on place i
          // selectedModel.save(sc, Utils.S3STORAGE + Utils.DMODELS + dMap.apply(d))
        } catch {
          case _: Throwable => sc.parallelize("dss".toSeq, 1).saveAsTextFile(Utils.HDFSSTORAGE + Utils.DSCORES + "Fails/" + dMap.apply(d)+System.currentTimeMillis().toString().replace(" ", "_"))
        }

        //TODO add function to choose candidates and evaluate on url level

        //TODO CHOOSE MODEL BY F
      } catch {
        case e: Throwable =>
          val errMsg = "model:  " + d + " " + e.getLocalizedMessage() + e.getMessage()
          sc.parallelize(List(errMsg), 1).saveAsTextFile(Utils.HDFSSTORAGE + Utils.DMODELS + "log/" + errMsg + d+System.currentTimeMillis().toString().replace(" ", "_"))
      }
    }
  } catch {
    case e: Throwable =>
      val errMsg = e.getLocalizedMessage() + e.getMessage()
      sc.parallelize(List(errMsg), 1).saveAsTextFile(Utils.HDFSSTORAGE + Utils.DMODELS + "log/" + errMsg + System.currentTimeMillis().toString().replace(" ", "_"))
  }
}