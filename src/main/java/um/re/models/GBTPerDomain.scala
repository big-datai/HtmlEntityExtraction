package um.re.models


import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.KryoSerializer

import org.apache.spark.mllib.feature.{HashingTF,IDF}
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

import um.re.transform.Transformer
import um.re.utils.{UConf}
import um.re.utils.Utils

object GBTPerDomain {
 def main(args:Array[String]){ 
  val conf_s = new SparkConf().setAppName("es").setMaster("yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val data = new UConf(sc, 200)
  val all = data.getData

  //val list = List("richtonemusic.co.uk","wholesalesupplements.shop.rakuten.com","shop.everythingbuttheweddingdress.com","DiscountCleaningProducts.com","yesss.co.uk","idsecurityonline.com","janitorialequipmentsupply.com","sanddollarlifestyles.com","protoolsdirect.co.uk","educationalinsights.com","faucet-warehouse.com","rexart.com","chronostore.com","racks-for-all.shop.rakuten.com","musicdirect.com","budgetpackaging.com","americanblinds.com","overthehill.com","thesupplementstore.co.uk","intheholegolf.com","alldesignerglasses.com","nitetimetoys.com","instrumentalley.com","ergonomic-chairs.officechairs.com","piratescave.co.uk")
  //val list = List("richtonemusic.co.uk")
  val list = args(0).split(",").filter(s=> !s.equals(""))
  
  var domain2ScoreMap : Map[String,IndexedSeq[(Int,(Long,Long,Long,Long,Double,Double,Double))]] = Map.empty 
  for(d <- list){
  
  val parsedDataPerURL = Transformer.parseDataPerURL(all).filter(l => l._2._4.equals(d)).groupBy(_._1)
  
  val splits = parsedDataPerURL.randomSplit(Array(0.7, 0.3))
  val (training, test) = (splits(0).flatMap(l=>l._2), splits(1).flatMap(l=>l._2))
  
  val hashingTF = new HashingTF(300000)
  val tf :RDD[Vector] = hashingTF.transform(training.map(l => l._2._2))
  val idf = (new IDF(minDocFreq = 10)).fit(tf)
  val idf_vector  = idf.idf.toArray
       
  val tfidf_avg = Statistics.colStats(idf.transform(tf)).mean.toArray
  val selected_indices = Transformer.getTopTFIDFIndices(100,tfidf_avg)
  val idf_vector_filtered = Transformer.projectByIndices(idf_vector, selected_indices) 
  
  val training_points = Transformer.data2pointsPerURL(training,idf_vector_filtered,selected_indices,hashingTF).map(p=> p._2).repartition(10)
  val test_points = Transformer.data2pointsPerURL(test,idf_vector_filtered,selected_indices,hashingTF).repartition(10)
  
  val boostingStrategy = BoostingStrategy.defaultParams("Classification")
  boostingStrategy.numIterations = 1 
  //boostingStrategy.treeStrategy.maxDepth = 5
  val model = GradientBoostedTrees.train(training_points, boostingStrategy)
 
  val subModels = Transformer.buildTreeSubModels(model)
  val scoresMap = subModels.map(m=>Transformer.evaluateModel(Transformer.labelAndPredPerURL(m,test_points),m))
  domain2ScoreMap = domain2ScoreMap.updated(d, scoresMap)   
  }
  
  val domain2ScoreList = domain2ScoreMap.toList
 
  sc.parallelize(domain2ScoreList, 1).saveAsTextFile("hdfs:///pavlovout/dscores/")
 
  /*for(d <- domain2ScoreMap.keySet){
    val scores = domain2ScoreMap.apply(d)
    for(i<- scala.collection.SortedSet[Int]() ++scores.keySet )
        println(d+" : "+i+" -- "+ scores.apply(i))
  }*/
  
 } 
}