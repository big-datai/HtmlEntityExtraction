package um.re.analysis

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.stat.Statistics
import um.re.transform.Transformer
import um.re.utils.Utils
import um.re.analysis.UConfAnal
import um.re.analysis.UConfAnal2
import um.re.transform.Transformer
import um.re.utils.{ UConf }
import um.re.utils.Utils
import um.re.analysis.EsExporter2

object FullR2S3 extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)

 
//Load html data with  Title
    val dataHtmls = new UConfAnal(sc, 150)
    //Read from ES
    val alldata = dataHtmls.getData

//Save2s3      
   alldata.saveAsObjectFile(Utils.S3STORAGE+"/dpavlov/es/full_river") 
}