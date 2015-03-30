package um.re.models

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

object ValidateModels extends App {
  
  
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)

  val data = new UConf(sc, 200)
  val all = data.getData
  //load Map of Domain->ModelKey
  val dMap = sc.textFile(("s3://rawd/objects/dmodels/dlist"),1).collect().mkString("\n").split("\n").map(l=> (l.split("\t")(0),l.split("\t")(1)) ).toMap
   
  var domain2ScoreMap: Map[String, IndexedSeq[(Int, (Long, Long, Long, Long, Double, Double, Double, Double, Double))]] = Map.empty

  for (d <- dMap) {
    val dom= d._1
    // filter domain group by url (url => Iterator.cadidates)
    val parsedDataPerURL = Transformer.parseDataPerURL(all).filter(l => l._2._4.equals(dom)).groupBy(_._1)
    val allData=parsedDataPerURL.flatMap(l => l._2)
    val hashingTF = new HashingTF(300000)
    val tf: RDD[Vector] = hashingTF.transform(allData.map(l => l._2._2))
    val idf = (new IDF(minDocFreq = 10)).fit(tf)
    val idf_vector = idf.idf.toArray
    val tfidf_avg = Statistics.colStats(idf.transform(tf)).mean.toArray
    val selected_indices = Transformer.getTopTFIDFIndices(100, tfidf_avg)
    val idf_vector_filtered = Transformer.projectByIndices(idf_vector, selected_indices)
    val all_points = Transformer.data2pointsPerURL(allData, idf_vector_filtered, selected_indices, hashingTF).repartition(10)

     //load model for domain dom from s3 and perform prediction
    val modKey= d._2
    val model = GradientBoostedTreesModel.load(sc, "s3://rawd/objects/dmodels/"+modKey)    
    val subModels = Transformer.buildTreeSubModels(model)
    val scoresMap = subModels.map(m => Transformer.evaluateModel(Transformer.labelAndPredPerURL(m,all_points), m))
    domain2ScoreMap = domain2ScoreMap.updated(dom, scoresMap)
    val domain2ScoreList = domain2ScoreMap.toList.map { l =>
      l._2.map { s => l._1 + " : " + s.toString }.mkString("\n")
    }
    //save results to s3://rawd/objects/res/
    sc.parallelize(domain2ScoreList, 1).saveAsTextFile("s3://rawd/objects/res/"+dom +"_"+modKey) // list on place i
  }
}