package um.re.es.emr

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
import um.re.es.emr.MyRegistrator
import um.re.es.emr.NumberFinder2
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

/**
 * This class is supposed to read and write data to ES and analyse it on EMR cluster
 */

object EmrSparkEs extends App {
  

  /**
   * This function takes ES source and transforms it to format of R candidates
   */
   def getCandidates(source2: RDD[(String, Map[String, String])]) = {
    val candid = source2.map { l =>
      try {
        val nf = NumberFinder2
        val id = l._2.get("url").toString
        val h = l._2.get("price_prop1").toString
        val res = nf.find(id, h)
        res
      } catch {
        case _: Exception => { "[{\"no\":\"data\"}]" }
      }
    }
    candid
  }

  //val conf = new Configuration()
  val reg = new MyRegistrator

  val conf = new JobConf()
  conf.set("es.resource", "htmls/data")
  conf.set("es.query", "?q=price_prop1:xml")
  conf.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")

  // conf.set("es.query", "{\"query\":{\"bool\":{\"must\":[{\"query_string\":{\"default_field\":\"data.price_prop1\",\"query\":\" xml:lang=\"en\"\"}},{\"query_string\":{\"default_field\":\"data.price_patterns\",\"query\":\"price \"}}],\"must_not\":[],\"should\":[]}},\"from\":0,\"size\":50,\"sort\":[],\"facets\":{}}")
  //{"query":{"bool":{"must":[{"query_string":{"default_field":"data.price_prop1","query":"<?xml version=\"1.0\""}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"facets":{}}
  //{"query":{"bool":{"must":[{"query_string":{"default_field":"data.price_prop1","query":" xml:lang=\"en\""}},{"query_string":{"default_field":"data.price_patterns","query":"price "}}],"must_not":[],"should":[]}},"from":0,"size":10,"sort":[],"facets":{}}

  //val conf_s = new SparkConf().setAppName("es").set("master", "yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  //val sc = new SparkContext(conf_s)

  val conf_s = new SparkConf().setAppName("es").setMaster("local[8]").set("spark.serializer", classOf[KryoSerializer].getName)
  conf_s.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf_s.set("spark.kryo.registrator", "um.re.es.emr.MyRegistrator")
  val sc = new SparkContext(conf_s)
  sc.hadoopConfiguration.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")
  sc.hadoopConfiguration.set("es.query", "?q=price_prop1:xml")
  sc.hadoopConfiguration.set("es.resource", "htmls/data")

  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])

  val source2 = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString) }.toMap) }.repartition(100)
  source2.partitions.size

  val source3 = source.map { l => (l._1.toString(), l._2.toString) }.repartition(100)

  val source15 = sc.makeRDD(source2.take(15))
  val candid15 = source15.map { l =>
    try {
      val nf = NumberFinder2
      val id = l._2.get("url").toString
      val h = l._2.get("price_prop1").toString
      val res = nf.find(id, h)
      res
    } catch {
      case _: Exception => { "[{\"no\":\"data\"}]" }
    }
  }

  val one = source2.take(1)
  val m = one.apply(0)._2.filterKeys(p =>
    if (p == "url" || p == "price_prop1")
      true
    else
      false)

  source.saveAsTextFile("hdfs:///spark-logs//raw1")
  source.coalesce(1, true).saveAsTextFile("hdfs:///spark-logs//raw")
  //http://wpcertification.blogspot.co.il/2014/08/how-to-use-elasticsearch-as-input-for.html
  val temp2 = sc.textFile("hdfs:///spark-logs/docs2/part-00000")

  val temp = sc.textFile("")

  Utils.printToFile(new File("//mnt//res.txt"))(p => {
    temp2.toArray.foreach(p.println)
  })



  /* 
  val stream = KafkaUtils.createStream[String, Message, StringDecoder, MessageDecoder](ssc, kafkaConfig, kafkaTopics, StorageLevel.MEMORY_AND_DISK).map(_._2)
  stream.foreachRDD(messageRDD => {
    /**
     * Live indexing of Kafka messages; note, that this is also
     * an appropriate place to integrate further message analysis
     */
    val messages = messageRDD.map(prepare)
    messages.saveAsNewAPIHadoopFile("-", classOf[NullWritable], classOf[MapWritable], classOf[EsOutputFormat], esConfig)

  })
*/
}