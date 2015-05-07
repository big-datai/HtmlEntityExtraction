package um.re.streaming

import java.util.Properties
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import scala.Array.canBuildFrom

object KafkaMsgProducer extends App {
  val sc = new SparkContext()
  val ssc = new StreamingContext(sc, Seconds(30))

  val Array(brokers, topic) = Array("localhost:9092", "testOut")

  // Zookeeper connection properties
  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.DefaultEncoder")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, Array[Byte]](config)

  val urls = scala.io.Source.fromFile("/Users/dmitry/untitled1000.txt").mkString.replaceAll("\"\n\"", "\"\r\"").replaceAll("\n", "").
    split("\r").filter(l => l.startsWith(""""full_river","data","""")).
    take(3).
    map { l =>
      val l1 = l.substring(""""full_river","data","""".length())
      l1.substring(0, l1.indexOf("\""))
    }.filter(!_.startsWith("http://www.the-house.com"))
  val msgs = urls.map { url =>
    var html = ""
    try {
      html = scala.io.Source.fromURL(url).mkString
    } catch {
      case _: Throwable => html = ""
    }
    new Msg(url,html,0,"test")
  }.filter{msg => !(msg.html.equals(""))}

  // Send some messages
  while (true) {
    val messages = msgs.map { msg =>
      new KeyedMessage[String, Array[Byte]](topic, msg.getBytes)
    }.toArray

    producer.send(messages: _*)
    Thread.sleep(100)
  }

}