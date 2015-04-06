package um.re.analysis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import um.re.utils.UConf
import um.re.utils.Utils
import um.re.analysis.UConfAnal2

object EsExporter2 extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)
  
  var dest = ""
  //if (args.apply(0)==null || args.apply(0).equals(""))
    dest = Utils.HDFSSTORAGE + Utils.ANALDATA
 // else
   // dest = args.apply(0)
    
  val data = new UConfAnal2(sc, 300)
  val all = data.getData
  
  // all.saveAsObjectFile(dest)

}