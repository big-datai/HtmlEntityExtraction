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
import java.util.concurrent.ConcurrentHashMap

object dima {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(596); 
  var count: AtomicLong = new AtomicLong;System.out.println("""count  : java.util.concurrent.atomic.AtomicLong = """ + $show(count ));$skip(43); 
  println("fox of the best fox in th gym");$skip(110); 
  val conf = new SparkConf().setAppName("hackaton").setMaster("local[1]").set("spark.executor.memory", "13g");System.out.println("""conf  : org.apache.spark.SparkConf = """ + $show(conf ));$skip(34); 
  val sc = new SparkContext(conf);System.out.println("""sc  : org.apache.spark.SparkContext = """ + $show(sc ));$skip(94); 

  val raw = sc.textFile("/Users//dmitry//Desktop//hackathon_data//RawData//data.merged.csv");System.out.println("""raw  : org.apache.spark.rdd.RDD[String] = """ + $show(raw ));$skip(244); 
  val raw_us_sgiggle = raw.filter(f => f.contains("US\",\"[{")).map(l => l.replaceAll("[{}\\[\\]\"]", "").
    replaceAll("install_time:\\d*", "").replaceAll("package_name:", "").replaceAll(" ", ",").replaceAll(",,", ",").dropRight(1)).take(2);System.out.println("""raw_us_sgiggle  : Array[String] = """ + $show(raw_us_sgiggle ));$skip(43); 

  raw_us_sgiggle.take(1).foreach(println);$skip(80); 

  var string2number: ConcurrentHashMap[String, String] = new ConcurrentHashMap;System.out.println("""string2number  : java.util.concurrent.ConcurrentHashMap[String,String] = """ + $show(string2number ));$skip(70); 
  var test: ConcurrentHashMap[String, String] = new ConcurrentHashMap;System.out.println("""test  : java.util.concurrent.ConcurrentHashMap[String,String] = """ + $show(test ));$skip(85); 
  //map

  for (line <- (raw_us_sgiggle ++ raw_us_sgiggle ++ raw_us_sgiggle)) {

  };$skip(421); 

  val only_num = (raw_us_sgiggle).map { l =>
    val line = l.split(",").map { w =>
      if (string2number.get(w) != None) {
        // println(string2number.get(w))
        string2number.get(w)
      } else {
        count.addAndGet(1)
        println("count:" + count.get() + " word mapping : " + w)
        string2number.put("w", count.get().toString)
        count.toString
      }
    }
    line.mkString(",")
  };System.out.println("""only_num  : Array[String] = """ + $show(only_num ));$skip(31); val res$0 = 
  string2number.get("android");System.out.println("""res0: String = """ + $show(res$0));$skip(18); val res$1 = 
  only_num.length;System.out.println("""res1: Int = """ + $show(res$1));$skip(22); val res$2 = 

  test.put("1", "2");System.out.println("""res2: String = """ + $show(res$2));$skip(21); val res$3 = 
  test.put("1", "3");System.out.println("""res3: String = """ + $show(res$3));$skip(8); val res$4 = 
  count;System.out.println("""res4: java.util.concurrent.atomic.AtomicLong = """ + $show(res$4));$skip(17); 

  println(test)}

  /*
  
  val conf = new SparkConf().setAppName("hackaton").setMaster("local[4]").set("spark.executor.memory", "10g")
  val sc = new SparkContext(conf)

  val raw = sc.textFile("/Users//dmitry//Desktop//hackathon_data//RawData//data.merged.csv")
  val ai = sc.textFile("/Users//dmitry//Desktop//hackathon_data//Additional_Data//also_installed.tsv")
  val app2id = sc.textFile("/Users//dmitry//Desktop//hackathon_data//Additional_Data//App2IdMap.tsv")

  val raw_us = raw.filter(f => f.contains("US\",\"[{")).flatMap(l => l.split("[,]")) //filter only US
  println(raw_us.count())
  raw_us.collect().take(10).foreach(println)

  //println (raw.join(ai))
*/
}
