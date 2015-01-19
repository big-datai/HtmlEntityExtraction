package um.re.es.emr


import java.io.File
import scala.collection.JavaConversions._
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.serializer.KryoSerializer
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.elasticsearch.spark
import play.api.libs.json._
import um.re.es.emr.NumberFinder2
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import play.api.libs.json.JacksonJson

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[JobConf])
    kryo.register(classOf[Text])
    kryo.register(classOf[MapWritable])
    kryo.register(classOf[JsObject])
  }
}