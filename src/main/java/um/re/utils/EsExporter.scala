package um.re.utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapred.JobConf

object EsExporter extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)
  
  def export(dest:String = Utils.HDFSSTORAGE + Utils.DCANDIDS) = {
	  val data = new UConf(sc, 300)
	  val all = data.getData
	  all.saveAsObjectFile(dest)
  }
  
}