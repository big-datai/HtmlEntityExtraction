package um.re.analytics

import com.utils.aws.AWSUtils
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.{SparkContext, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import um.re.utils.Utils


/**
  * @author mike
  */
object Kafka2CompPerUser {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    var (timeInterval, brokers, inputTopic, fromOffset, outputPath) = ("", "", "", "", "")
    if (args.size == 5) {
      timeInterval = args(0)
      brokers = args(1)
      inputTopic = args(2)
      fromOffset = args(3)
      outputPath = args(4)

    } else {
      timeInterval = "60"
      brokers = "54.225.122.3:9092"
      fromOffset = "smallest"
      inputTopic = "preseeds"
      outputPath = "file:///home/ec2-user/storesReport/CompPerUser/output"

      conf.setMaster("local[*]")
    }

    // try getting inner IPs
    try {
      val brokerIP = brokers.split(":")(0)
      val brokerPort = brokers.split(":")(1)
      val innerBroker = AWSUtils.getPrivateIp(brokerIP) + ":" + brokerPort
      brokers = innerBroker
    } catch {
      case e: Exception => {
        println("#?#?#?#?#?#?#  Couldn't get inner broker IP, using : " + brokers +
          "\n#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }

    val sc = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(timeInterval.toInt))

    val msgCounter = ssc.sparkContext.accumulator(0L, "msgCounter")
    val storesPerUserCounter = ssc.sparkContext.accumulator(0L, "storesPerUserCounter")
    val usersCounter = ssc.sparkContext.accumulator(0L, "usersCounter")
    try {

      // Create direct kafka stream with brokers and topics
      val topicsSet = inputTopic.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> fromOffset)

      val input = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc, kafkaParams, topicsSet)
      val parsed = Utils.parseBigMessage(input)

      val storesPerUser = parsed.map {
        case (msg, msgMap) =>
          msgCounter += 1
          val gglName = msgMap.apply("gglName")
          val userId = msgMap.apply("upc").trim()
          (userId, gglName)
      }.groupByKey().map {
        case (userId, stores) =>
          usersCounter += 1
          storesPerUserCounter += stores.size
          val storeSet = stores.toSet.toList.sorted
          userId + "," + storeSet.mkString(",")
      }
      storesPerUser.transform { rdd => rdd.coalesce(1, false) }
        .saveAsTextFiles(outputPath)

    } catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(timeInterval.toInt * 1000)
    ssc.stop(false)
  }
}
