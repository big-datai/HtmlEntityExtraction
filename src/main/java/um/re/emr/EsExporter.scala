package um.re.emr

import org.apache.spark.{SparkConf, SparkContext}
import um.re.utils.{UConf, Utils}

object EsExporter extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)
  val data = new UConf(sc, 300)
  //if (args.apply(0)==null || args.apply(0).equals(""))
  dest = Utils.HDFSSTORAGE + Utils.DCANDIDS
  // else
  // dest = args.apply(0)
  val all = data.getData
  var dest = ""
  all.saveAsObjectFile(dest) //"s3://rawd/objects/dcandids/")//"dest)

}