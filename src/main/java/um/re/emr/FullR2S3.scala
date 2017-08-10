package um.re.emr

import org.apache.spark.{SparkConf, SparkContext}
import um.re.utils.{UConf, Utils}


object FullR2S3 extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)


  //Load html data with  Title
  val dataHtmls = new UConf(sc, 150)
  //Read from ES
  val alldata = dataHtmls.getData

  //Save2s3
  alldata.saveAsObjectFile(Utils.S3STORAGE + "/dpavlov/es/source20150513")
}