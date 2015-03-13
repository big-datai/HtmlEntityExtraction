package um.re.models

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.hadoop.io.MapWritable
import org.apache.spark.SparkConf
import org.apache.hadoop.mapred.JobConf
import org.elasticsearch.hadoop.mr.EsInputFormat
import scala.Array.canBuildFrom
import scala.collection.JavaConversions.mapAsScalaMap
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.serializer.KryoSerializer
import um.re.utils.Utils
import org.elasticsearch.hadoop.mr.EsInputFormat
import scala.collection.concurrent.TrieMap
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel


object GBTPerDomain {
    val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val conf = new JobConf()
  conf.set("es.resource", "candidl5/data"/*um.re.utils.EsUtils.ESINDEX*/)
  conf.set("es.nodes", "ec2-54-166-235-206.compute-1.amazonaws.com"/*um.re.utils.EsUtils.ESIP*/)
  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val all = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString()) }.toMap) }//.repartition(600)
  //merge text before and after

  //val domains = all.map{l=> Utils.getDomain(l._2.apply("url"))}.distinct(600).zipWithIndex
  //Array((Natex.us,0), (simplybeach.com,1), (soscleanroom.com,2), (very.co.uk,3), (caseswill.com,4))
  def parseData(raw:RDD[(String,Map[String,String])]) = {
    //val domain_map = domains
    raw.map { l =>
    val before = Utils.tokenazer(l._2.apply("text_before"))
    val after = Utils.tokenazer(l._2.apply("text_after"))
    val domain = Utils.getDomain(l._2.apply("url"))
    val location = Integer.valueOf(l._2.apply("location")).toDouble
    val parts = before ++ after //, location) 
    val parts_embedded = parts //.filter { w => (!w.isEmpty() && w.length > 3) }.map { w => w.toLowerCase }
    if ((l._2.apply("priceCandidate").contains(l._2.apply("price"))))
      (1, parts_embedded,location,domain)
    else
      (0, parts_embedded,location,domain)
  }.filter(l => l._2.length > 1 && l._4.equals("Natex.us"))
  }
	  
  val parsedData = parseData(all)
  //domains.unpersist()
	  
  val splits = parsedData.randomSplit(Array(0.7, 0.3))
  val (training, test) = (splits(0), splits(1))

  val hashingTF = new HashingTF(300000)
  val tf :RDD[Vector] = hashingTF.transform(training.map(l => l._2))
  val idf = (new IDF(minDocFreq = 10)).fit(tf)
  val idf_vector  = idf.idf.toArray
   	
  val k = math.min(100,idf_vector.filter(v=>v!=0.0).length) //number of tdudf features
  val tfidf_stats = Statistics.colStats(idf.transform(tf))
  val tfidf_avg = tfidf_stats.mean.toArray
  val tfidf_avg_sorted = tfidf_stats.mean.toArray.clone
  java.util.Arrays.sort(tfidf_avg_sorted)
  val top_k_value = tfidf_avg_sorted.takeRight(k)(0)
   	
  val selected_indices = (for(i <- tfidf_avg.indices if tfidf_avg(i) >= top_k_value ) yield i).toArray
  val idf_vector_filtered = selected_indices.map(i => idf_vector(i)) 
  
  def data_to_points(data:RDD[(Int,Seq[String],Double,String)]) = {
    val idf_vals = idf_vector_filtered
    val tf_model = hashingTF
    val selected_ind_vals = selected_indices 
    data.map{ case(lable,txt,location,domain)=>
      val tf_vals_full = tf_model.transform(txt).toArray
      val tf_vals = selected_ind_vals.map(i=> tf_vals_full(i))
      val tfidf_vals = (tf_vals,idf_vals).zipped.map((d1,d2)=>d1*d2)
      val features = tfidf_vals++Array(location)
      val values = features.filter{l=>l!=0}
      val index = features.zipWithIndex.filter{l=> l._1!=0 }.map{l=>l._2}
      LabeledPoint(lable,Vectors.sparse(features.length,index, values))  
    }
  }
  
  val training_points = data_to_points(training).repartition(10)
  val test_points = data_to_points(test)
  
  val boostingStrategy = BoostingStrategy.defaultParams("Classification")
  boostingStrategy.numIterations = 40 
  //boostingStrategy.treeStrategy.maxDepth = 5
  val model = GradientBoostedTrees.train(training_points, boostingStrategy)
    
 val algo = model.algo
 val trees = model.trees
 val treeW = model.treeWeights
 val numTrees = trees.length
 var scoresMap : Map[Int,(Long,Long,Long,Long,Double,Double,Double)] = Map.empty
 for( i <- List(5,10,20,30,40) ){
   val model_i = new GradientBoostedTreesModel(algo, trees.take(i), treeW.take(i))
 
   def labelAndPred(input_points:RDD[LabeledPoint])={
    val local_model = model_i
	  val labelAndPreds = input_points.map { point =>
	  val prediction = local_model.predict(point.features)
	  (point.label, prediction)
	  }
   labelAndPreds
   }  
  
  val labelAndPreds = labelAndPred(test_points)
  val tp = labelAndPreds.filter{case (l,p) => (l==1)&&(p==1) }.count
  val tn = labelAndPreds.filter{case (l,p) => (l==0)&&(p==0) }.count
  val fp = labelAndPreds.filter{case (l,p) => (l==0)&&(p==1) }.count
  val fn = labelAndPreds.filter{case (l,p) => (l==1)&&(p==0) }.count
  val sen = tp / (tp + fn).toDouble
  val spec = tn / (fp + tn).toDouble
  val prec = tp / (tp + fp).toDouble
  scoresMap = scoresMap.updated(i,(tp,tn,fp,fn,sen,spec,prec))
 }
    
 for(i<- 0 to scoresMap.size-1 )
   println("numTrees : "+i+" -- "+ scoresMap.apply(i))

  
//  val overAllPerformance = performancePerDomain.map(p=>p._2).fold((0,0,0,0))((r,c)=>(r._1+c._1,r._2+c._2,r._3+c._3,r._4+c._4))
  
  
  
}