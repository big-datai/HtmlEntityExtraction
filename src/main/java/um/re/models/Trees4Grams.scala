package um.re.models

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD
import um.re.transform.Transformer
import um.re.utils.{UConf, Utils}

object Trees4Grams {

  def main(args: Array[String]) {

    val conf_s = new SparkConf().setAppName("Trees4Grams")
    //.set("spark.serializer", classOf[KryoSerializer].getName)
    val sc = new SparkContext(conf_s)

    val parts = 2000
    val data = new UConf(sc, parts)
    val all = data.getData

    // all.repartition(parts).saveAsObjectFile("/user/hadoop/data_es2")

    val trees = Integer.parseInt(args.apply(0)) //50
    val grams = Integer.parseInt(args.apply(1))
    val grams2 = Integer.parseInt(args.apply(2))
    val fetures = Integer.parseInt(args.apply(3)) //10000
    val depth = 5

    val allSampled = all.sample(false, 0.1, 12345)

    allSampled.partitions.size //parseGramsTFIDFData
    val (trainingAll, testAll) = Transformer.splitRawDataByURL(allSampled)
    //val trainingData = Transformer.parseGramsTFIDFData(trainingAll, grams, grams2).repartition(parts)
    //val test = Transformer.parseGramsTFIDFData(testAll, grams, grams2).repartition(parts)
    val trainingData = Transformer.parseGramsTFIDFData(trainingAll, grams, grams2).repartition(parts)
    val test = Transformer.parseGramsTFIDFData(testAll, grams, grams2).repartition(parts)

    trainingData.partitions.size
    test.partitions.size
    //trainng idf
    val hashingTF = new HashingTF(500000)
    val tf: RDD[Vector] = hashingTF.transform(trainingData.map(l => l._2))
    val idf = (new IDF(minDocFreq = 10)).fit(tf)
    val idf_vector = idf.idf.toArray

    val tfidf_avg = Statistics.colStats(idf.transform(tf)).mean.toArray
    val selected_indices = Transformer.getTopTFIDFIndices(fetures, tfidf_avg)
    val idf_vector_filtered = Transformer.projectByIndices(idf_vector, selected_indices)

    val training_points = Transformer.data2points(trainingData, idf_vector_filtered, selected_indices, hashingTF).repartition(parts)
    val test_points = Transformer.data2points(test, idf_vector_filtered, selected_indices, hashingTF).repartition(parts)

    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = trees
    boostingStrategy.treeStrategy.maxDepth = depth ///4-8
    val model = GradientBoostedTrees.train(training_points, boostingStrategy)
    // Evaluate model on test instances and compute test error
    val res = Transformer.labelAndPredRes(test_points, model).replaceAll("\\s", "_").replaceAll(":", "_")
    Utils.write2File("trees_" + trees + "_grams_" + grams + "_grams2_" + grams2 + "_fetures_" + fetures + "_res_" + res, sc)
  }
}