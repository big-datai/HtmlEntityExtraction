package um.re.streaming

import java.util.Properties
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object KafkaProducer extends App {

  val sc = new SparkContext()
  val ssc = new StreamingContext(sc, Seconds(5))

  val Array(brokers, topic,messagesPerSec,wordsPerMessage) = Array("localhost:9092", "testOut","10","3")

  // Zookeeper connection properties
  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)

  // Send some messages
  while (true) {
    val messages = (1 to messagesPerSec.toInt).map { messageNum =>
      val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
        .mkString(" ")

      new KeyedMessage[String, String](topic, str)
    }.toArray

    producer.send(messages: _*)
    Thread.sleep(100)
  }

}