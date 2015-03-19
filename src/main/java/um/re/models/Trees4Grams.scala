package um.re.models

import um.re.utils.UConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import um.re.transform.Transformer
import org.apache.spark.serializer.KryoSerializer

object Trees4Grams {

  val conf_s = new SparkConf().setAppName("es").setMaster("yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val data = new UConf(sc, 1000)
  val all = data.getData
  val parsedData = Transformer.parseDataNGram(all)
  
}