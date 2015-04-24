package um.re.streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import kafka.producer._
import org.apache.spark.streaming.Seconds
import java.util.Properties

object KafkaConsProd extends App {
  val sc = new SparkContext()

  val ssc = new StreamingContext(sc, Seconds(5))

  val Array(brokers, topics) = Array("localhost:9092", "test2")

  // Create direct kafka stream with brokers and topics
  val topicsSet = topics.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
  val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, topicsSet)

  messages.print

  //  def toKMsg(v:String) = { new KeyedMessage[String, String]("testOut", v)}

  messages.map(v=>v._2).foreachRDD { rdd =>
    rdd.foreachPartition { p =>
      val props = new Properties()
      props.put("metadata.broker.list", brokers)
      props.put("serializer.class", "kafka.serializer.StringEncoder")

      @transient val config = new ProducerConfig(props)
      @transient val producer = new Producer[String, String](config)
      p.foreach(rec => producer.send(new KeyedMessage[String, String]("testOut", rec)))
      producer.close()
    }
  }

  ssc.start()
  ssc.awaitTermination()

}