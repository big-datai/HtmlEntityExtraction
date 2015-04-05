package um.re.emr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import um.re.utils.UConf
import um.re.utils.Utils
import um.re.transform.Transformer
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.Vector

object DomainDataForModels extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)
  try {

    val data = new UConf(sc, 300)
    val all = data.getData

    val dMap = sc.textFile((Utils.S3STORAGE + Utils.DMODELS + "dlist"), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1))).toMap
    val parsed = Transformer.parseDataPerURL(all).repartition(300).cache
    val list = dMap.keySet.toList
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

        training_points.saveAsObjectFile("/pard/" + dMap.apply(d) + "/training_points/")
        test_points.saveAsObjectFile("/pard/" + dMap.apply(d) + "/test_points/")

      }
    }
  } catch {
      case e: Throwable =>
        val errMsg = e.getLocalizedMessage() + e.getMessage()
        sc.parallelize(List(errMsg), 1).saveAsTextFile(Utils.HDFSSTORAGE + "/pard/"  + "log/" + errMsg + System.currentTimeMillis().toString().replace(" ", "_"))
    }

}