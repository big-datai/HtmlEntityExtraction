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


object MakeLPEran extends App {

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val conf = new JobConf()
  conf.set("es.resource", "candidl/data")
  conf.set("es.nodes", "ec2-54-145-93-208.compute-1.amazonaws.com")
  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val all_all = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString()) }.toMap) }.repartition(600)
  //merge text before and after

  
  //Split data for testing
  
 //  val sp = all_all.randomSplit(Array(0.9, 0.1),seed=111)
  // val (no_keeping, keeping) = (sp(0), sp(1))
  // val all=keeping
  val all=all_all
  
  val parsedData = all.map { l =>
    val before = Utils.tokenazer(l._2.apply("text_before"))
    val after = Utils.tokenazer(l._2.apply("text_after"))
    val domain = Utils.getDomain(l._2.apply("url"))
    val location = Integer.valueOf(l._2.apply("location")).toDouble
    val parts = before ++ after ++ Array(domain) //, location) 
    val parts_embedded = parts //.filter { w => (!w.isEmpty() && w.length > 3) }.map { w => w.toLowerCase }
    if ((l._2.apply("priceCandidate").contains(l._2.apply("price"))))
      (1, parts_embedded,location)
    else
      (0, parts_embedded,location)
  }.filter(l => l._2.length > 1)
  parsedData.take(10).foreach(println)

  val splits = parsedData.randomSplit(Array(0.7, 0.3))
  val (trainingData, test) = (splits(0), splits(1))

  //trainng idf
  val hashingTF = new HashingTF(50000)
  val tf_pos: RDD[Vector] = hashingTF.transform(trainingData.filter(t=> t._1==1).map(l => l._2))
  val tf_neg: RDD[Vector] = hashingTF.transform(trainingData.filter(t=> t._1==0).map(l => l._2))
  val idf_pos = (new IDF(minDocFreq = 10)).fit(tf_pos)
  val idf_vector_pos  = idf_pos.idf.toArray
  val idf_neg = (new IDF(minDocFreq = 10)).fit(tf_neg)
  val idf_vector_neg  = idf_neg.idf.toArray
  
  
  //val tfidf = idf.transform(tf)
  //val idf_vector  = idf.idf.toArray
  
  def data_to_points_pos(data:RDD[(Int,Seq[String],Double)]) = {
    val idf_vals = idf_vector_pos
    val tf_model = hashingTF
    data.map{case(lable,txt,location)=>
      val tf_vals = tf_model.transform(txt).toArray
      val tfidf_vals = (tf_vals,idf_vals).zipped.map((d1,d2)=>d1*d2)
      val features = tfidf_vals++Array(location)
      val values = features.filter{l=>l!=0}
      val index = features.zipWithIndex.filter{l=> l._1!=0 }.map{l=>l._2}
      LabeledPoint(lable,Vectors.sparse(features.length,index, values))
      
    }
  }
  
  def data_to_points_neg(data:RDD[(Int,Seq[String],Double)]) = {
    val idf_vals = idf_vector_neg
    val tf_model = hashingTF
    data.map{case(lable,txt,location)=>
      val tf_vals = tf_model.transform(txt).toArray
      val tfidf_vals = (tf_vals,idf_vals).zipped.map((d1,d2)=>d1*d2)
      val features = tfidf_vals++Array(location)
      val values = features.filter{l=>l!=0}
      val index = features.zipWithIndex.filter{l=> l._1!=0 }.map{l=>l._2}
      LabeledPoint(lable,Vectors.sparse(features.length,index, values))
      
    }
  }
  
  
  
  val training_points_pos = data_to_points_pos(trainingData.filter(t=> t._1==1))
  val training_points_neg = data_to_points_neg(trainingData.filter(t=> t._1==0))
  val training_points=training_points_pos ++ training_points_neg
  
  //test tfidf
  
  //val tf_test: RDD[Vector] = hashingTF.transform(test.map(l => l._2))
  
  
    //val hashingTF = new HashingTF(50000)
  val tf_test: RDD[Vector] = hashingTF.transform(test.map(l => l._2))

  val idf = (new IDF(minDocFreq = 10)).fit(tf_test)
  //val tfidf = idf.transform(tf)
  val idf_vector  = idf.idf.toArray
  
  def data_to_points(data:RDD[(Int,Seq[String],Double)]) = {
    val idf_vals = idf_vector
    val tf_model = hashingTF
    data.map{case(lable,txt,location)=>
      val tf_vals = tf_model.transform(txt).toArray
      val tfidf_vals = (tf_vals,idf_vals).zipped.map((d1,d2)=>d1*d2)
      val features = tfidf_vals++Array(location)
      val values = features.filter{l=>l!=0}
      val index = features.zipWithIndex.filter{l=> l._1!=0 }.map{l=>l._2}
      LabeledPoint(lable,Vectors.sparse(features.length,index, values))
      
    }
  }
  
  
  val test_points = data_to_points(test)
  
  val boostingStrategy =
    BoostingStrategy.defaultParams("Classification")
  boostingStrategy.numIterations = 3 // Note: Use more in practice
  boostingStrategy.treeStrategy.maxDepth = 5
  val model =
    GradientBoostedTrees.train(training_points, boostingStrategy)

  // Evaluate model on test instances and compute test error
 def labelAndPred(input_points:RDD[LabeledPoint])={
    val local_model = model
	  val labelAndPreds = input_points.map { point =>
	  val prediction = local_model.predict(point.features)
	  (point.label, prediction)
	  }
   labelAndPreds
    
  }
  val labelAndPreds_train = labelAndPred(training_points)
  val tp_trn = labelAndPreds_train.filter{case (l,p) => (l==1)&&(p==1) }.count
  val tn_trn = labelAndPreds_train.filter{case (l,p) => (l==0)&&(p==0) }.count
  val fp_trn = labelAndPreds_train.filter{case (l,p) => (l==0)&&(p==1) }.count
  val fn_trn = labelAndPreds_train.filter{case (l,p) => (l==1)&&(p==0) }.count

  
  val labelAndPreds_test = labelAndPred(test_points)
  val tp_tst = labelAndPreds_test.filter{case (l,p) => (l==1)&&(p==1) }.count
  val tn_tst = labelAndPreds_test.filter{case (l,p) => (l==0)&&(p==0) }.count
  val fp_tst = labelAndPreds_test.filter{case (l,p) => (l==0)&&(p==1) }.count
  val fn_tst = labelAndPreds_test.filter{case (l,p) => (l==1)&&(p==0) }.count

  
  
  /*def tf_to_tfidf(rdd:RDD[Vector]):RDD[Vector] ={
    val idf_vals = idf_vector
    rdd.map{tf_vals => 
      		val tfidfArr = (tf_vals.toArray,idf_vals).zipped.map((d1,d2)=>d1*d2)
      		val values = tfidfArr.filter{l=>l!=0}
      		val index = tfidfArr.zipWithIndex.filter{l=> l._1!=0 }.map{l=>l._2}
      		Vectors.sparse(50000,index, values)}
  }
  
  //training tfidf
  val tf_train_p: RDD[Vector] = hashingTF.transform(trainingData.filter(t=> t._1==1).map(l => l._2))
  val tf_train_n: RDD[Vector] = hashingTF.transform(trainingData.filter(t=> t._1==0).map(l => l._2))
  
  val tfidf_train_p = tf_to_tfidf(tf_train_p)
  val tfidf_train_n = tf_to_tfidf(tf_train_n)
  
  val positive_train = tfidf_train_p.map { l => LabeledPoint(1, l) }
  val negat_train = tfidf_train_n.map { l => LabeledPoint(0, l) }
  //ALL TOGETHER
  val points_train = positive_train ++ negat_train
  
  points_train.partitions.size
  //test tfidf
  
  val tf_test_p: RDD[Vector] = hashingTF.transform(test.filter(t=> t._1==1).map(l => l._2))
  val tf_test_n: RDD[Vector] = hashingTF.transform(test.filter(t=> t._1==0).map(l => l._2))
  
 
  val tfidf_test_p = tf_to_tfidf(tf_test_p)
  val tfidf_test_n = tf_to_tfidf(tf_test_n)
  
  val positive = tfidf_test_p.map { l => LabeledPoint(1, l) }
  val negat = tfidf_test_n.map { l => LabeledPoint(0, l) }
  //ALL TOGETHER
  val points_test = positive ++ negat
  //points_train.repartition(192)
  // train model
  val boostingStrategy =
    BoostingStrategy.defaultParams("Classification")
  boostingStrategy.numIterations = 40 // Note: Use more in practice
  boostingStrategy.treeStrategy.maxDepth = 2
  val model =
    GradientBoostedTrees.train(points_train, boostingStrategy)

  // Evaluate model on test instances and compute test error
 def labelAndPred(input_points:RDD[LabeledPoint])={
    val local_model = model
	  val labelAndPreds = input_points.map { point =>
	  val prediction = local_model.predict(point.features)
	  (point.label, prediction)
	  }
   labelAndPreds
    
  }
  val labelAndPreds = labelAndPred(points_test)
  val tp = labelAndPreds.filter{case (l,p) => (l==1)&&(p==1) }.count
  val tn = labelAndPreds.filter{case (l,p) => (l==0)&&(p==0) }.count
  val fp = labelAndPreds.filter{case (l,p) => (l==0)&&(p==1) }.count
  val fn = labelAndPreds.filter{case (l,p) => (l==1)&&(p==0) }.count
  */
  //SAVE TO FILE ALL DATA
  /*MLUtils.saveAsLibSVMFile(points, "hdfs:///pavlovout/points")
  MLUtils.saveAsLibSVMFile(points, "s3://pavlovout/points")
  // val tf_ppoints = tf.map { l => LabeledPoint(1, l) } ++ tfn.map { l => LabeledPoint(0, l) }
  //SAVE TO FILE ALL DATA
  MLUtils.saveAsLibSVMFile(points, "hdfs:///pavlovout/points")
  test.saveAsTextFile("hdfs:///pavlovout/test_text")
  test.saveAsObjectFile("hdfs:///pavlovout/test_obj")
  val l = sc.textFile("hdfs:///pavlovout/test_text")
  val ll = sc.objectFile("hdfs:///pavlovout/test_obj")
*/
  
}