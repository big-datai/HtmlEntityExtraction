package um.re.emr

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import um.re.transform.Transformer
import um.re.utils.Utils

object BuildAndSaveModels extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)
  try {

    val ilist = List("1036", "1076", "1160", "1193", "1250", "1280", "1349", "1354", "14", "1409", "1411", "1412", "1429", "1434", "1436", "1446", "1448", "1452", "1468", "1518", "1522", "1531", "1562", "1573", "1577", "1578", "1590", "1628", "1655", "1666", "1677", "1686", "1730", "1749", "1761", "1766", "1777", "1795", "1831", "1843", "1853", "1897", "1909", "1919", "1933", "1948", "1995", "2066", "207", "2106", "2116", "2118", "2124", "2130", "2141", "2153", "2180", "2190", "2202", "2253", "2268", "2302", "2323", "2383", "2433", "2455", "2456", "2516", "252", "2529", "2541", "2583", "2600", "2609", "2621", "2639", "2665", "2684", "2739", "2860", "2872", "2879", "2897", "2904", "2923", "2941", "2954", "2964", "3066", "3090", "3094", "3104", "3113", "3117", "3143", "3146", "3150", "3151", "3153", "3165", "3188", "3207", "3223", "3254", "3265", "3273", "3297", "3382", "340", "342", "3432", "3442", "3512", "3595", "3611", "3621", "3644", "3687", "3712", "3720", "3732", "3735", "3749", "3764", "3824", "3835", "3865", "3911", "3949", "3954", "3962", "3975", "4058", "4115", "4146", "4175", "4179", "4279", "4290", "4301", "4303", "4337", "4360", "4445", "4473", "4549", "458", "4581", "46", "4618", "463", "4693", "4700", "4762", "4789", "4801", "4827", "4842", "4876", "4895", "4915", "4973", "5011", "505", "511", "611", "659", "670", "672", "758", "775", "790", "802", "810", "841", "842", "843", "855", "886", "910", "919", "92")
    val dPairs = sc.textFile((Utils.S3STORAGE + Utils.DMODELS + "dlist"), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1)))
    val dMap = dPairs.toMap
    val dMapR = dPairs.map(_.swap).toMap

    val list = ilist.map(i => dMapR.apply(i))
    val parList = list.par
    //parList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(25))

    for (d <- parList) {
      try {
        sc.parallelize(list, 1).saveAsTextFile("/pard/list/" + dMap.apply(d) + System.currentTimeMillis().toString().replace(" ", "_"))
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("                                " + d + "                                          ")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

        val training_points = sc.objectFile[LabeledPoint](Utils.S3STORAGE + "/pard/" + dMap.apply(d) + "/training_points/", 10)
        val test_points = sc.objectFile[(String, LabeledPoint)](Utils.S3STORAGE + "/pard/" + dMap.apply(d) + "/test_points/", 10)

        val boostingStrategy = BoostingStrategy.defaultParams("Classification")
        boostingStrategy.numIterations = 30
        boostingStrategy.treeStrategy.maxDepth = 5
        val model = GradientBoostedTrees.train(training_points, boostingStrategy)

        val scores = Transformer.evaluateModel(Transformer.labelAndPredPerURL(model, test_points), model)

        val scoreString = d + " : " + scores.toString

        try {
          println("-----------------------------entering the models " + d + " - " + scoreString.length + " ---------------------------------------------------")
          sc.parallelize(Seq(scoreString), 1).saveAsTextFile("/pard/" + dMap.apply(d) + "/dscores/", classOf[GzipCodec]) // list on place i
          sc.parallelize(Seq(model)).saveAsObjectFile("/pard/" + dMap.apply(d) + "/selectedModel/")
          println("--------------------------------------------------------------------------------")
          println("--------------------------------------------------------------------------------")
          println("--------------------------------------------------------------------------------")
          println("                            " + "/pard/" + dMap.apply(d) + "                    ")
          println("--------------------------------------------------------------------------------")
          println("--------------------------------------------------------------------------------")
          println("--------------------------------------------------------------------------------")
          sc.parallelize(Seq(scoreString), 1).saveAsTextFile(Utils.S3STORAGE + "/pard/" + dMap.apply(d) + "/dscores/", classOf[GzipCodec]) // list on place i
          sc.parallelize(Seq(model)).saveAsObjectFile(Utils.S3STORAGE + "/pard/" + dMap.apply(d) + "/selectedModel/")
        } catch {
          case _: Throwable => sc.parallelize("dss".toSeq, 1).saveAsTextFile(Utils.HDFSSTORAGE + Utils.DSCORES + "Fails/" + dMap.apply(d) + System.currentTimeMillis().toString().replace(" ", "_"))
        }

      }
    }
  } catch {
    case e: Throwable =>
      val errMsg = e.getLocalizedMessage() + e.getMessage()
      sc.parallelize(List(errMsg), 1).saveAsTextFile(Utils.HDFSSTORAGE + "/pard/" + "log/" + errMsg + System.currentTimeMillis().toString().replace(" ", "_"))
  }
}