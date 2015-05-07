package um.re.streaming

import java.util.Properties
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import scala.reflect.io.Streamable.Bytes

object KafkaProducer extends App {

  val sc = new SparkContext()
  val ssc = new StreamingContext(sc, Seconds(5))

  val Array(brokers, topic,messagesPerSec,wordsPerMessage) = Array("localhost:9092", "testOut","1","4")

  // Zookeeper connection properties
  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.DefaultEncoder")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, Array[Byte]](config)

  // Send some messages
  while (true) {
    val messages = (1 to messagesPerSec.toInt).map { messageNum =>
      val msg = (1 to wordsPerMessage.toInt).map(x => new Msg("http://url.com","<html></html>",scala.util.Random.nextInt(10),"rand") )
        .toList.head.getBytes

      new KeyedMessage[String, Array[Byte]](topic, msg)
    }.toArray

    producer.send(messages: _*)
    Thread.sleep(100)
  }

}