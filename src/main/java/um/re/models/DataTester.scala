package um.re.models

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import um.re.utils.UConf
import um.re.transform.Transformer
import org.apache.spark.serializer.KryoSerializer


object DataTester extends App {
  
  val conf = new SparkConf().set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf)
  val data = new UConf(sc, 600)
  val all = data.getData
  val parsedData = Transformer.parseData4Test(all)
  
  

}