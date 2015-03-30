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


object GBTByURL extends App {
  val conf_s = new SparkConf().setAppName("es").setMaster("yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val trees = 3 // Integer.parseInt(args.apply(0)) 
  val grams = 3 // Integer.parseInt(args.apply(1))  
  val grams2 = 0// Integer.parseInt(args.apply(2))
  val fetures = 100 // Integer.parseInt(args.apply(3)) //10000
  val tokenize = false // Integer.parseInt(args.apply(4))==1
  val subModelSizes = args.apply(5).split(",").map(Integer.parseInt(_))
  val depth = 5

  
  val data = new UConf(sc, 300)
  val all = data.getData
  
  val urls = Transformer.parseDataRawPerURL(all).map(l=>(l._1,1)).sample(false, 0.1, 12345)
  val parsedDataRawPerURL = Transformer.parseDataRawPerURL(all).join(urls).map(l=> (l._1,l._2._1)).repartition(300)
  val parsedDataPerURL = Transformer.tokenizeParsedDataByURL(parsedDataRawPerURL,tokenize,grams,grams2).repartition(2000).groupBy(_._1)
  val splits = parsedDataPerURL.randomSplit(Array(0.7, 0.3))
  val (training, test) = (splits(0).flatMap(l=>l._2), splits(1).flatMap(l=>l._2))
  
  val hashingTF = new HashingTF(300000)
  val tf :RDD[Vector] = hashingTF.transform(training.map(l => l._2._2))
  val idf = (new IDF(minDocFreq = 10)).fit(tf)
  val idf_vector  = idf.idf.toArray
       
  val tfidf_avg = Statistics.colStats(idf.transform(tf)).mean.toArray
  val selected_indices = Transformer.getTopTFIDFIndices(100,tfidf_avg)
  val idf_vector_filtered = Transformer.projectByIndices(idf_vector, selected_indices) 
  
	  val training_points = Transformer.data2pointsPerURL(training,idf_vector_filtered,selected_indices,hashingTF).map(p=> p._2).repartition(300)
	  val test_points = Transformer.data2pointsPerURL(test,idf_vector_filtered,selected_indices,hashingTF).repartition(300)
	  
	  val boostingStrategy = BoostingStrategy.defaultParams("Classification")
	  boostingStrategy.numIterations = 2	 
	  //boostingStrategy.treeStrategy.maxDepth = 5
	  val model = GradientBoostedTrees.train(training_points, boostingStrategy)
 
  val subModels = Transformer.buildTreeSubModels(model)
  val scoresMap = subModels.map(m=>Transformer.evaluateModel(Transformer.labelAndPredPerURL(m,test_points),m))

  	

}