package um.re.bin

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import org.apache.spark.serializer.KryoSerializer

object GradientBoostedF extends App {
  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  // Load and parse the data file.
  val trainingData =
    MLUtils.loadLibSVMFile(sc, "hdfs:///pavlovout/pointsALL")
    val test =
    MLUtils.loadLibSVMFile(sc, "hdfs:///pavlovout/testALL")
    // Split data into training/test sets
 // val splits = data.randomSplit(Array(0.7, 0.3))
 // val (trainingData, test) = (splits(0), splits(1))
  val testData = test//sc.makeRDD(test.take(1000))
  trainingData.cache
  testData.cache
  // Train a GradientBoostedTrees model.
  //categoricalFeaturesInfo  Map(0 -> 2, 4 -> 10) specifies that feature 0 is binary (taking values 0 or 1) and that feature 4 has 10 categories (values {0, 1
  val boostingStrategy =
    BoostingStrategy.defaultParams("Classification")
  boostingStrategy.numIterations = 3 // Note: Use more in practice
  val model =
    GradientBoostedTrees.train(trainingData, boostingStrategy)

  // Evaluate model on test instances and compute test error
  val labelAndPreds = testData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }

  val testSuccess = labelAndPreds.map { point =>
    if (point._1 == point._2) 1.0 else 0.0
  }.mean()

  val real_predic = labelAndPreds.map { point =>
    if (point._1 != point._2 && point._1 == 0.0) {
      (point._1, point._2)
    } else
      null
  } filter { l => l != null }

  real_predic.take(10000).foreach(println)

  println("Precision = " + testSuccess)
  println("Learned GBT model:\n" + model.toDebugString)
  
  
  def saveModel(path:String, model:GradientBoostedTrees){
  //save model 
  import java.io.FileOutputStream 
  import java.io.ObjectOutputStream 
  val fos = new FileOutputStream("/home/hadoop/modelAll") 
  val oos = new ObjectOutputStream(fos)   
  oos.writeObject(model)   
  oos.close
  }
  
  def loadModel(){
  import java.io.FileInputStream 
  import java.io.ObjectInputStream 
  var model:org.apache.spark.mllib.tree.model.GradientBoostedTreesModel=null
  val fos = new FileInputStream("/home/hadoop/modelAll") 
  val oos = new ObjectInputStream(fos) 
   model = oos.readObject().asInstanceOf[org.apache.spark.mllib.tree.model.GradientBoostedTreesModel]

  }
  
  
}