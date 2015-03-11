package um.re.utils

import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.serializer.KryoRegistrator
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import play.api.libs.json._
import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf

class URegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[org.apache.hadoop.mapred.JobConf])
    kryo.register(classOf[org.apache.hadoop.io.Text])
    kryo.register(classOf[org.apache.hadoop.io.MapWritable])
  }
}