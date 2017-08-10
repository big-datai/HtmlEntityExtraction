package um.re.emr

import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import um.re.utils.EsUtils

/**
  * this class is to import data from ES to S3
  */
object ES2S3 extends App {

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)
  val conf = new JobConf()
  conf.set("es.resource", EsUtils.ESINDEX)
  conf.set("es.nodes", EsUtils.ESIP)
  EsUtils.es2s3("candidl", sc)

}