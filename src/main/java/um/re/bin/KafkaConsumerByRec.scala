package um.re.bin

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.Seconds

object KafkaConsumerByRec {
  val sc = new SparkContext()

  val ssc = new StreamingContext(sc, Seconds(5))
  
  val Array(zkQuorum, group, topics, numThreads) = Array("localhost:2181","testOut" ,"test2","1")
  ssc.checkpoint("checkpoint")

  val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
 lines.print
 
  ssc.start()
  ssc.awaitTermination()
  
}