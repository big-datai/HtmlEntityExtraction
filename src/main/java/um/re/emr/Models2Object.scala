package um.re.emr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

/**
 * @author mike
 */
object Models2Object {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
    var (dMapPath, modelsPath, outputPath) = ("", "", "")
    if (args.size == 3) {
      dMapPath = args(0)
      modelsPath = args(1)
      outputPath = args(2)
    } else {
      dMapPath = "/Users/mike/umbrella/dMapNew"
      modelsPath = "/Users/mike/umbrella/Models/"
      outputPath = "/Users/mike/umbrella/ModelsObject"
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)
    try {
      val hash = new scala.collection.immutable.HashMap
      val dHashMap = hash ++ sc.textFile((dMapPath), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1))).toMap

      val modelsMap: Map[String, (GradientBoostedTreesModel, Array[Double], Array[Int])] = dHashMap.mapValues { domainCode =>
        val (model, idf, selected_indices) = sc.objectFile[(GradientBoostedTreesModel, Array[Double], Array[Int])](modelsPath + domainCode + "/part-00000", 1).first
        (model, idf, selected_indices)
      }
      val modelsHashMap = hash ++ modelsMap
      val rdd = sc.parallelize(Seq(modelsHashMap), 1)
      rdd.saveAsObjectFile(outputPath)
    } catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
  }
}