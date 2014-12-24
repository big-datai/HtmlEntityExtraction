package scala.utils
import scala.Array.canBuildFrom
import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo.Classification
import org.apache.spark.mllib.tree.impurity.Gini
import scala.util.control.Exception.allCatch
import scala.mvn._
import com.naivebase.Tokenizer

object Train {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(566); 
  println("Welcome to the Scala worksheet");$skip(91); 
  
  
  val texCSV=Source.fromFile("C:\\Users\\dmitry.pavlov\\Desktop\\query_result2.csv");System.out.println("""texCSV  : scala.io.BufferedSource = """ + $show(texCSV ));$skip(76); 
  
  val tokens= com.naivebase.Tokenizer.tokenize(texCSV.getLines mkString);System.out.println("""tokens  : Seq[String] = """ + $show(tokens ))}
  
  
  
}
