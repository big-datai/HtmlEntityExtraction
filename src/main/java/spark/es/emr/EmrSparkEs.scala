package spark.es.emr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.MapWritable
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.apache.hadoop.io.Text
import org.apache.spark.serializer.KryoSerializer
import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter

/**
 * This class is supposed to read and write data to ES and analyse it on EMR cluster
 */
object EmrSparkEs extends App {

  val conf = new Configuration()
  conf.set("es.resource", "htmls/data")
  conf.set("es.query", "?q=prod_id:11202409")
  conf.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
   val sc = new SparkContext(conf_s)

 
  val esRDD = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable]).cache
  val docCount = esRDD.count()

  println(docCount)
  println(esRDD.toArray.toString)
  esRDD.collect().foreach(println)
  
  /*
  val file = new File("hdfs:///spark-logs//domains.json");
  if (!file.exists()) {
    file.createNewFile();
  }
  val fw = new FileWriter(file.getAbsoluteFile());

  val bw = new BufferedWriter(fw);
  bw.write("++++++++++++++++++++++++++++++++++++++++++++++++++++         " + docCount);
  bw.close();
  * */
  

  print("++++++++++++++++++++++++++++++++++++++++++++++++++++         " + docCount)
}