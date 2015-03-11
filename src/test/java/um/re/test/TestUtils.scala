package um.re.test

import java.io.File
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import um.re.utils.URegistrator
import scala.math
import scala.collection.JavaConversions._
import play.api.libs.json._
import play.api.libs.json.{ Json, JsValue, JsObject, JsArray }
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.elasticsearch.spark
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.SparkContext
import org.apache.hadoop.io.NullWritable
import org.elasticsearch.hadoop.mr.EsOutputFormat
import um.re.utils
import um.re.utils.Utils

object TestUtils extends App {

  val nn = Utils.skipSpecialCharsInPattern(" n class=\"saleprice\">$<span class=\"price\"> (.*?)</span> </span>  <meta itemprop=\"priceC|||")

  println(nn)
  val remove = Utils.threePlusTrim("    a          b          c        asdf     lkjasdf          asdlkjas;dkljf  ")
  println(remove)
  /*
  val conf_s = new SparkConf().setAppName("es").setMaster("local[8]")//.set("spark.serializer", classOf[KryoSerializer].getName)
  conf_s.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf_s.set("spark.kryo.registrator", "um.re.es.emr.MyRegistrator")
  val sc = new SparkContext(conf_s)
 
  val conf = new JobConf()
  conf.set("es.resource", "htmls/data")
  conf.set("es.query", "?q=prod_id:23799864")
  conf.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")

  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
  val source2 = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString) }.toMap) }
  val one = source2.take(1)
  val one_json = one.apply(0)._2.filterKeys(p =>
    if (p == "url" || p == "price_prop1" || p == "price_patterns")
      true
    else
      false)
  
      val res = Utils.extPatternLocationPair(Utils.threePlusTrim(one_json.get("price_patterns").get.toString.dropRight(4)), Utils.threePlusTrim(one_json.get("price_prop1").get.toString), 150)
val res2 = Utils.extPatternLocationPair((one_json.get("price_patterns").get.toString.dropRight(4)), (one_json.get("price_prop1").get.toString), 150)
   */

  val source4 = scala.io.Source.fromFile("file.html")//("sample.html")
  val lines = source4.mkString
  source4.close()

  val source3 = scala.io.Source.fromFile("pattern_sample.txt")
  val lines2 = source3.mkString
  source3.close()

  //val ress = Utils.extPatternLocationPair(Utils.replaceS(Utils.threePlusTrim(lines2.dropRight(4))), Utils.replaceS(Utils.threePlusTrim(lines.toString)), 150)
  
  val ress =Utils.allPatterns(lines2,lines,150)
  println("++++++++++++")
  println(ress)

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

//Files.write(Paths.get("file.txt"), html.getBytes(StandardCharsets.UTF_8))
}

