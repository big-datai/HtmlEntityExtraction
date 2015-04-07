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

object GBTPerDomainPar {
  def main(args: Array[String]) {
    val conf_s = new SparkConf()
    
    val sc = new SparkContext(conf_s)
    val processID = Integer.valueOf(args(0))
    val numDomains = Integer.valueOf(args(1))
    try {

      val data = new UConf(sc, 300)
      val all = data.getData

      //val list = List("richtonemusic.co.uk","wholesalesupplements.shop.rakuten.com","shop.everythingbuttheweddingdress.com","DiscountCleaningProducts.com","yesss.co.uk","idsecurityonline.com","janitorialequipmentsupply.com","sanddollarlifestyles.com","protoolsdirect.co.uk","educationalinsights.com","faucet-warehouse.com","rexart.com","chronostore.com","racks-for-all.shop.rakuten.com","musicdirect.com","budgetpackaging.com","americanblinds.com","overthehill.com","thesupplementstore.co.uk","intheholegolf.com","alldesignerglasses.com","nitetimetoys.com","instrumentalley.com","ergonomic-chairs.officechairs.com","piratescave.co.uk")
      //val list = List("fawnandforest.com","parrotshopping.com").par
      //val list = List("mrcostumes.com", "sto00.mailcar.net", "parentsfavorite.com", "wildbirdstoreonline.com", "runningboardsdirect.com", "vitaminworlddiscount.shop.rakuten.com", "gigaworld.co.uk", "samstores.com", "galaxorstore.com", "flyshack.com", "eventstable.com", "shop.texasmediasystems.com", "vcdiscounter.com", "safetycompany.com", "early-pregnancy-tests.com", "grandfatherclockco.com", "letsplaysomething.com", "livingdirect.com", "golflocker.com", "totalfitnessbath.co.uk", "ecodirect.com", "ettitude.com", "tacticalgear.com", "housemakers.co.uk", "uncommongoods.com", "retrobikegear.com", "shopallergy.com", "inkstation.com.au", "cymbalfusion.com", "toolschest.com", "BlueRainbowDesign.com", "ge.factoryoutletstore.com", "nostalgicbulbs.com", "wellbots.com", "rugstudio.com", "dsmusic.com", "Natex.us", "nbcuniversalstore.com", "heatingcontrolsonline.co.uk", "webosolar.com", "footaction.com", "waterfiltersfast.com", "liquidsurfandsail.com", "yourdiscountchemist.com.au", "petguys.com", "careandheal.com", "ctl.net", "batterymart.com", "lift-chair-store.com", "scaledynasty.com", "sneakers4u.com")
      //list of domains 
      //TODO create list of domains that are relevant

      val dMap = sc.textFile((Utils.S3STORAGE + Utils.DMODELS + "dlist"), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1))).toMap
      val parsed = Transformer.parseDataPerURL(all).repartition(300).cache
      val modDivider = dMap.size / numDomains
      //val list = args(0).split(",").filter(s => !s.equals("")).filter(dMap.keySet.contains(_))
      val list = dMap.keySet.toArray.sorted.zipWithIndex.filter { d => d._2 % modDivider == processID }.map(d => d._1).toList
      //take(20)

      //val list=sc.textFile("/domains.list").flatMap{l=>l.split(",").filter(s => !s.equals("")).filter(dMap.keySet.contains(_))}.filter(s => !s.equals("")).toArray().toList
      val parList = list.par
      //parList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(25))

      for (d <- parList) {
        try {
          sc.parallelize(list, 1).saveAsTextFile("/mike" + processID + "/list/" + dMap.apply(d) + System.currentTimeMillis().toString().replace(" ", "_"))
          println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
          println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
          println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
          println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
          println("                                " + d + "                                          ")
          println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
          println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
          println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
          // filter domain group by url (url => Iterator.cadidates)

          val parsedDataPerURL = parsed.repartition(300).filter(l => l._2._4.equals(d)).groupBy(_._1).repartition(10)

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
            println("-----------------------------entering the models " + d + " - " + scoreString.length + " ---------------------------------------------------")
            sc.parallelize(scoreString, 1).saveAsTextFile(Utils.HDFSSTORAGE + "/mike" + processID + Utils.DSCORES + dMap.apply(d) + System.currentTimeMillis().toString().replace(" ", "_")) // list on place i
            sc.parallelize(Seq(selectedModel)).saveAsObjectFile(Utils.HDFSSTORAGE + "/mike" + processID + Utils.DMODELS + dMap.apply(d) + System.currentTimeMillis().toString().replace(" ", "_"))
            println("--------------------------------------------------------------------------------")
            println("--------------------------------------------------------------------------------")
            println("--------------------------------------------------------------------------------")
            println("                                " + "/mike" + Utils.DSCORES + d + System.currentTimeMillis().toString().replace(" ", "_") + "                                          ")
            println("--------------------------------------------------------------------------------")
            println("--------------------------------------------------------------------------------")
            println("--------------------------------------------------------------------------------")
            sc.parallelize(scoreString, 1).saveAsTextFile(Utils.S3STORAGE + Utils.DSCORES + dMap.apply(d), classOf[GzipCodec]) // list on place i
            sc.parallelize(Seq(selectedModel)).saveAsObjectFile(Utils.S3STORAGE + Utils.DMODELS + dMap.apply(d))
          } catch {
            case _: Throwable => sc.parallelize("dss".toSeq, 1).saveAsTextFile(Utils.HDFSSTORAGE + Utils.DSCORES + "Fails/" + dMap.apply(d) + System.currentTimeMillis().toString().replace(" ", "_"))
          }

          //TODO add function to choose candidates and evaluate on url level

          //TODO CHOOSE MODEL BY F
        } catch {
          case e: Throwable =>
            val errMsg = "model:  " + d + " " + e.getLocalizedMessage() + e.getMessage()
            sc.parallelize(List(errMsg), 1).saveAsTextFile(Utils.HDFSSTORAGE + "/mike" + processID + Utils.DMODELS + "log/" + errMsg + d + System.currentTimeMillis().toString().replace(" ", "_"))
        }
      }
    } catch {
      case e: Throwable =>
        val errMsg = e.getLocalizedMessage() + e.getMessage()
        sc.parallelize(List(errMsg), 1).saveAsTextFile(Utils.HDFSSTORAGE + "/mike" + processID + Utils.DMODELS + "log/" + errMsg + System.currentTimeMillis().toString().replace(" ", "_"))
    }
  }
}