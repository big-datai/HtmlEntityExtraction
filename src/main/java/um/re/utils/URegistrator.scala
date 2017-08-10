package um.re.utils

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class URegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[org.apache.hadoop.mapred.JobConf])
    kryo.register(classOf[org.apache.hadoop.io.Text])
    kryo.register(classOf[org.apache.hadoop.io.MapWritable])
  }
}