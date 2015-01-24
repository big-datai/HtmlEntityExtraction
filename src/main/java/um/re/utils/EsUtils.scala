package um.re.utils
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

object EsUtils {

  val conf = new JobConf()
  conf.set("es.resource", "process_count/counter")
  conf.set("es.query", "?q=updatePriceCount")
  conf.set("es.nodes", "ec2-54-167-216-26.compute-1.amazonaws.com")
  /**
   * This method should write to ES using elasticsearch.spark
   */
  def write2ES(exit:RDD[String],sc:SparkContext){
    //Writing back to ES
  val json1 = "{\"job\" : \"my job 1\", \"process_count\" : \"5\"}"
  val json2 = "{\"job\" : \"my job 2\", \"process_count\" : \"20\"}"
  val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])

  EsSpark.saveToEs(sc.makeRDD(Seq(json1, json2)), "process_count/counter")
  EsSpark.saveToEs(source, "process_count/counter")  
    
  }
    /**
   * This method should write to ES using hadoop style
   */
  //http://www.elasticsearch.org/guide/en/elasticsearch/hadoop/current/mapreduce.html
  def write2ESHadoop(source:RDD[(Text,MapWritable)],sc:SparkContext){
    //Writing back to ES
  
    source.saveAsNewAPIHadoopFile("-", classOf[NullWritable], classOf[MapWritable], classOf[EsOutputFormat], conf)
    //sc.makeRDD(Seq(json1, json2)).saveAsNewAPIHadoopFile("-", classOf[NullWritable], classOf[MapWritable], classOf[EsOutputFormat], conf)

  }
}