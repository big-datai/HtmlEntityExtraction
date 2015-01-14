package um.re.es.emr

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
import collection.JavaConversions._
import play.api.libs.json.Json
import org.elasticsearch.spark.rdd.EsSpark
import um.re.es.emr.NumberFinder
//import org.elasticsearch.spark

/**
 * This class is supposed to read and write data to ES and analyse it on EMR cluster
 */
object EmrSparkEs extends App {

  val conf = new Configuration()
  conf.set("es.resource", "htmls/data")
  //conf.set("es.query", "?q=prod_id:11202409")
  conf.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  // val conf_s = new SparkConf().setAppName("es").setMaster("local").set("spark.serializer", classOf[KryoSerializer].getName)
  // val sc = new SparkContext(conf_s)

  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable]).cache

  val docs = source.map { hit =>
    {
      val nf = NumberFinder
      val id = hit._1.toString()
      val dc = hit._2.toString
      val html_start = dc.indexOf("price_prop1=")
      val html_end = dc.indexOf("price_prop_anal=")
      val html = dc.substring(html_start, html_end)
      val price_id = dc.indexOf("price_updated=")
      val price_id_end = dc.length() - 2
      val price = dc.substring(price_id, price_id_end)
      val res = nf.find(id, html)
      res.toString
    }
  }
  docs.cache
  docs.count

  docs.coalesce(1, true).saveAsTextFile("hdfs:///spark-logs//docs1")

  printToFile(new File("//mnt//res.txt"))(p => {
    docs.foreach(p.println)
  })

  //back to RDD
  // val par = sc.parallelize(re)

  //par.saveAsTextFile("hdfs:///spark-logs//pc.json")

  //not parallel calculation without RDD
  /*
  //leaves it RDD format
  val docs4 = source.map { hit =>
    {
       val nf = NumberFinder
      val id = hit._1.toString()
      val dc = hit._2.toString
      val html_start = dc.indexOf("price_prop1=")
      val html_end = dc.indexOf("price_prop_anal=")
      val html = dc.substring(html_start, html_end)
      val price_id = dc.indexOf("price_updated=")
      val price_id_end = dc.length() - 2
      val price = dc.substring(price_id, price_id_end)
      
      (id, html)
    }
  }.collect

  val re = docs4.map {
    l =>
      val nf = NumberFinder
      val id = l._1
      val r = l._2
      nf.find(id, r).toString

  }
  
  val json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\"}"
  val json2 = "{\"participants\" : 5,\"airport\" : \"OTP\"}"

  EsSpark.saveJsonToEs(sc.makeRDD(Seq(json1, json2)), "htt/docs")

  EsSpark.saveJsonToEs(par, "htmls/docs")

  EsSpark.saveToEs(par, "gogo/docs")
 */
  //sc.makeRDD(Seq(numbers, airports)).saveToEs("htmls/docs")
  // println(Json.parse(docs.apply(1)._2) + "++++++++++++++++++")

  /*
  //println(docCount)
  println(source.toArray.toString)
  source.collect().foreach(println)

  println("                    +++++++++++++++++++++++++++++++++++++++                    ")
 
  //org.apache.hadoop.io.Text, org.apache.hadoop.io.MapWritable
  source.map {
    unit =>
      val url = unit._2
      println(url )
  }
*/
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

  // print("++++++++++++++++++++++++++++++++++++++++++++++++++++         " + docCount)
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
}