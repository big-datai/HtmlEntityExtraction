package um.re.emr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import um.re.utils.UConf
import um.re.utils.Utils

object EsExporter extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)
  var dest = ""
  if (args.apply(0)==null || args.apply(0).equals(""))
    dest = Utils.HDFSSTORAGE + Utils.DCANDIDS
  else
    dest = args.apply(0)
    
  val data = new UConf(sc, 300)
  val all = data.getData
  all.saveAsObjectFile(dest)

}