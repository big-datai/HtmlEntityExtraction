package um.re.emr

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import um.re.transform.Transformer
import um.re.utils.{UConf, Utils}

object DomainDataForModels extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)
  try {

    val data = new UConf(sc, 100)
    val all = data.getData


    val ilist = List("1036", "1076", "1160", "1193", "1250", "1280", "1349", "1354", "14", "1409", "1411", "1412", "1429", "1434", "1436", "1446", "1448", "1452", "1468", "1518", "1522", "1531", "1562", "1573", "1577", "1578", "1590", "1628", "1655", "1666", "1677", "1686", "1730", "1749", "1761", "1766", "1777", "1795", "1831", "1843", "1853", "1897", "1909", "1919", "1933", "1948", "1995", "2066", "207", "2106", "2116", "2118", "2124", "2130", "2141", "2153", "2180", "2190", "2202", "2253", "2268", "2302", "2323", "2383", "2433", "2455", "2456", "2516", "252", "2529", "2541", "2583", "2600", "2609", "2621", "2639", "2665", "2684", "2739", "2860", "2872", "2879", "2897", "2904", "2923", "2941", "2954", "2964", "3066", "3090", "3094", "3104", "3113", "3117", "3143", "3146", "3150", "3151", "3153", "3165", "3188", "3207", "3223", "3254", "3265", "3273", "3297", "3382", "340", "342", "3432", "3442", "3512", "3595", "3611", "3621", "3644", "3687", "3712", "3720", "3732", "3735", "3749", "3764", "3824", "3835", "3865", "3911", "3949", "3954", "3962", "3975", "4058", "4115", "4146", "4175", "4179", "4279", "4290", "4301", "4303", "4337", "4360", "4445", "4473", "4549", "458", "4581", "46", "4618", "463", "4693", "4700", "4762", "4789", "4801", "4827", "4842", "4876", "4895", "4915", "4973", "5011", "505", "511", "611", "659", "670", "672", "758", "775", "790", "802", "810", "841", "842", "843", "855", "886", "910", "919", "92")
    val dPairs = sc.textFile((Utils.S3STORAGE + Utils.DMODELS + "part-00000"), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1)))
    val dMap = dPairs.toMap
    val dMapR = dPairs.map(_.swap).toMap

    val list = ilist.map(i => dMapR.apply(i))
    val parList = list.par
    //parList.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(25))
    val partByDomain = new HashPartitioner(100)
    val parsed = Transformer.parseDataPerURL(all).map { l => (l._2._4, l) }.partitionBy(partByDomain).mapPartitions({ p => p.map(_._2) }, true).cache

    for (d <- parList) {
      try {
        sc.parallelize(list, 1).saveAsTextFile("/temp/pard/list/" + dMap.apply(d) + System.currentTimeMillis().toString().replace(" ", "_"))
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("                                " + d + "                                          ")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

        val partByURL = new HashPartitioner(10)
        // filter domain group by url (url => Iterator.cadidates)
        val parsedDataPerURL = parsed.repartition(300).filter(l => l._2._4.equals(d)).groupBy(_._1).partitionBy(partByURL)

        val splits = parsedDataPerURL.randomSplit(Array(0.7, 0.3))
        val (training, test) = (splits(0).flatMap(l => l._2), splits(1).flatMap(l => l._2))

        val hashingTF = new HashingTF(30000)
        val tf: RDD[Vector] = hashingTF.transform(training.map(l => l._2._2))
        val idf = (new IDF(minDocFreq = 10)).fit(tf)
        val idf_vector = idf.idf.toArray

        val tfidf_avg = Statistics.colStats(idf.transform(tf)).mean.toArray
        val selected_indices = Transformer.getTopTFIDFIndices(100, tfidf_avg)
        val idf_vector_filtered = Transformer.projectByIndices(idf_vector, selected_indices)

        val training_points = Transformer.data2pointsPerURL(training, idf_vector_filtered, selected_indices, hashingTF).map(p => p._2).repartition(10)
        val test_points = Transformer.data2pointsPerURL(test, idf_vector_filtered, selected_indices, hashingTF).repartition(10)

        training_points.saveAsObjectFile("/temp/pard/" + dMap.apply(d) + "/training_points/")
        test_points.saveAsObjectFile("/temp/pard/" + dMap.apply(d) + "/test_points/")

      }
    }
  } catch {
    case e: Throwable =>
      val errMsg = e.getLocalizedMessage() + e.getMessage()
      sc.parallelize(List(errMsg + "\n" + e.getStackTraceString), 1).saveAsTextFile(Utils.HDFSSTORAGE + "/temp/pard/" + "log/" + errMsg + System.currentTimeMillis().toString().replace(" ", "_"))
  }

}