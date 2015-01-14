package perion.hackaton

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.collection.immutable.HashMap
import scala.collection.breakOut
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import java.util.concurrent.atomic.AtomicLong
import scala.collection.immutable.TreeMap
import scala.util.control.Exception.allCatch
import java.io._
import scala.collection.concurrent._
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LassoWithSGD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.RidgeRegressionWithSGD

object HMain {
  var count: AtomicLong = new AtomicLong
  count.addAndGet(1)

  def isNumber(s: String): Boolean = (allCatch opt s.toLong).isDefined
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
  def map2Id(one: String, a2i: org.apache.spark.rdd.RDD[(String, String)]): String = {

    for (name <- a2i.toArray) {
      if (name._1 == one) {
        return name._2
      }
    }
    return one
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("hackaton").setMaster("local[4]").set("spark.executor.memory", "13g")
    val sc = new SparkContext(conf)

    // val lp=MLUtils.loadLibSVMFile(sc,"label_points")
    //lp.collect.foreach(println)

    val raw = sc.textFile("/Users//dmitry//Desktop//hackathon_data//RawData//data.merged.csv")
    // val ai = sc.textFile("/Users//dmitry//Desktop//hackathon_data//Additional_Data//also_installed.tsv")
    //val app2id = sc.textFile("/Users//dmitry//Desktop//hackathon_data//Additional_Data//App2IdMap.tsv")
    //val a2i = app2id.map(i => (i.split("	")(0), i.split("	")(1)))
    // val accum = sc.accumulator(0)
    //val a2imap: Map[String, String] = a2i.toArray.map(l => (l._1, l._2))(breakOut)

    val raw_us_sgiggle = raw.filter(f => f.contains("US\",\"[{")) /*.filter(l => l.contains("com.sgiggle.production"))*/ .map(l => l.replaceAll("[{}()\\[\\]\"]", "").
      replaceAll("install_time:\\d*", "").replaceAll("\\d\\d:\\d\\d:\\d\\d", "").replaceAll("package_name:", "").replaceAll(" ", ",").replaceAll(",,", ",").dropRight(1)).cache

    val wordCounts = raw_us_sgiggle.flatMap(line => line.split(",")).map(word => (word, 1)).reduceByKey((a, b) => a + b)

    println(raw_us_sgiggle.toArray.length)

    var string2number: TrieMap[String, Long] = new TrieMap

    this.synchronized {
      var counter = 1;
      raw_us_sgiggle.toArray.foreach { l =>
        l.split(",").map { w =>
          if (string2number.putIfAbsent(w, counter) == null)
            counter = counter + 1
        }
      }
    }
    //Creating a dictionary with a unique number for each word 
    var c = 1;
    val dic = string2number.map { l =>
      c = c + 1
      (l._1, c)
    }
    val wordIdf: TrieMap[Long, Int] = new TrieMap[Long, Int]
    wordCounts.toArray.map { p =>
      wordIdf.putIfAbsent(dic.apply(p._1).toLong, p._2)
    }

    printToFile(new File("tfIdf.txt"))(p => {
      wordIdf.foreach(p.println)
    })

    //Print mapping to a file
    printToFile(new File("mapping.txt"))(p => {
      dic.foreach(p.println)
    })

    val only_num = raw_us_sgiggle.filter(l => !l.isEmpty).map[String] { l =>
      val line = l.split(",").map { w =>
        dic.apply(w)
      }
      line.mkString(",")
    }
    //Printing samples of data
    raw_us_sgiggle.take(3).foreach(println)
    only_num.take(3).foreach(println)

    println("raw_us_sgiggle : " + raw_us_sgiggle.toArray.length)
    println("only numbers   : " + only_num.toArray.length)
    val size = 150000

    val parsedData = only_num.filter(l => if (l != null) { true } else { false }).map { line =>
      val parts = line.split(',')
      parts.map { v =>
        if (isNumber(v) == false) {
          println(v + "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        }
      }
      if (line == null) {
        println("line is null:!!!!" + line)
      }
      if (line.contains(dic.apply("com.sgiggle.production").toString()))
        LabeledPoint(1, Vectors.sparse(size, parts.map(l => l.toInt), parts.map(l => 1.0 / math.log(size / wordIdf.apply(l.toLong)))))
      else
        LabeledPoint(0, Vectors.sparse(size, parts.map(l => l.toInt), parts.map(l => 1.0 / math.log(size / wordIdf.apply(l.toLong)))))
    }
    //parsedData.persist
    //save the label points
    //MLUtils.saveAsLibSVMFile(parsedData, "label_points")

    /*
   printToFile(new File("parsed.txt"))(p => {
      parsedData.foreach(p.println)
    })
 
 */
    //println(parsedData.count)

    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 20
    val model = LogisticRegressionWithSGD.train(training, numIterations)
    //model.clearThreshold()

    // Evaluate model on training examples and compute training error
    val labelAndPreds = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    //labelAndPreds.foreach(println)

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / test.count
    val true_positive = labelAndPreds.filter(r => (r._1 == 1) && (r._2 == 1)).count.toDouble
    val true_negative = labelAndPreds.filter(r => (r._1 == 0) && (r._2 == 0)).count.toDouble
    val true_positive_false_negative = labelAndPreds.filter(r => (r._1 == 1)).count.toDouble
    val true_negative_false_positive = labelAndPreds.filter(r => (r._1 == 0)).count.toDouble
    val sensitivity = true_positive / true_positive_false_negative
    val specificity = true_negative / true_negative_false_positive

    println("\nTest Error = " + testErr)
    println("Sensitivity = " + sensitivity)
    println("Specificity = " + specificity)
    println();

    println("+++++++++++++++++++++++++++++++++++++++  next model +++++++++++++++++++++++++++++++")

    val model2 = RidgeRegressionWithSGD.train(training, numIterations)
    /*
     * SVMWithSGD
	LogisticRegressionWithSGD
	LinearRegressionWithSGD
	RidgeRegressionWithSGD
	LassoWithSGD
     */
    //model.clearThreshold()

    // Evaluate model on training examples and compute training error
    val labelAndPreds2 = test.map { point =>
      val prediction = model2.predict(point.features)
      (point.label, prediction)
    }

    //labelAndPreds.foreach(println)

    val testErr2 = labelAndPreds2.filter(r => r._1 != r._2).count.toDouble / test.count
    val true_positive2 = labelAndPreds2.filter(r => (r._1 == 1) && (r._2 == 1)).count.toDouble
    val true_negative2 = labelAndPreds2.filter(r => (r._1 == 0) && (r._2 == 0)).count.toDouble
    val true_positive_false_negative2 = labelAndPreds2.filter(r => (r._1 == 1)).count.toDouble
    val true_negative_false_positive2 = labelAndPreds2.filter(r => (r._1 == 0)).count.toDouble
    val sensitivity2 = true_positive2 / true_positive_false_negative2
    val specificity2 = true_negative2 / true_negative_false_positive2

    println("\nTest Error = " + testErr2)
    println("Sensitivity = " + sensitivity2)
    println("Specificity = " + specificity2)
    println();

    printToFile(new File("results.txt"))(p => {
      // dic.foreach(p.println)
      p.append("LogisticRegressionWithSGD ")
      p.append("\nTest Error  = " + testErr)
      p.append("Sensitivity = " + sensitivity)
      p.append("Specificity = " + specificity)

      p.append("RidgeRegressionWithSGD ")
      p.append("\nTest Error  = " + testErr2)
      p.append("Sensitivity = " + sensitivity2)
      p.append("Specificity = " + specificity2)
    })

    /*
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(labelAndPreds)
    val auROC = metrics.areaUnderROC()
    println("Area under ROC = " + auROC)
    */
  }
}