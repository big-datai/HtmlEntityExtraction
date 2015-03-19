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
  val parsedData = Transformer.parseData4Test(all).filter(c => c._2._6.equals("nitetimetoys.com")).map{
    case (url, (label,price,priceCandidate, normalizedLocation,parts_embedded, domain)) =>
    	(label,price,priceCandidate, normalizedLocation,domain,url)}
  val pos = parsedData.filter(c => c._1 == 1)
  val neg = parsedData.filter(c => c._1 == 0)
  
  val l = new java.util.Locale("de","DE")
  val p = java.text.NumberFormat.getNumberInstance(l).parse("1,99")

}