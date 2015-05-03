package um.re.streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import kafka.producer._
import org.apache.spark.streaming.Seconds
import java.util.Properties
import kafka.serializer.DefaultDecoder

object KafkaConsumer extends App {
  val sc = new SparkContext()

  val ssc = new StreamingContext(sc, Seconds(5))

  val Array(brokers, topics) = Array("localhost:9092", "testOut")

  // Create direct kafka stream with brokers and topics
  val topicsSet = topics.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
  val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
    ssc, kafkaParams, topicsSet).map(msg=> new Msg(msg._2))

  messages.print

  ssc.start()
  ssc.awaitTermination()

}