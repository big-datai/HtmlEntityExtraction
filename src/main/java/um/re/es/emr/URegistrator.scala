package um.re.es.emr

import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.serializer.KryoRegistrator

import com.esotericsoftware.kryo.Kryo

import play.api.libs.json.JsObject

class URegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[JobConf])
    kryo.register(classOf[Text])
    kryo.register(classOf[MapWritable])
    kryo.register(classOf[JsObject])
  }
}