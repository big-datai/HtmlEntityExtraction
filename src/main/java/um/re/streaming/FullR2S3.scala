package um.re.streaming

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import um.re.utils.Utils
import um.re.utils.{ UConf }
import um.re.utils.Utils


object FullR2S3 extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)

 
//Load html data with  Title
    val dataHtmls = new UConf(sc, 150)
    //Read from ES
    val alldata = dataHtmls.getData

//Save2s3      
   alldata.saveAsObjectFile(Utils.S3STORAGE+Utils.FULLR2S3) 
}