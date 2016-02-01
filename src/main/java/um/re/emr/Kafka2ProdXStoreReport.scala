package um.re.emr

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
import um.re.utils.Utils
import com.utils.messages.BigMessage
import com.utils.aws.AWSUtils
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.PairDStreamFunctions

/**
 * @author mike
 */

object Kafka2ProdXStoreReport {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    var (timeInterval, brokers, inputTopic, fromOffset, path2StoresReport) = ("", "", "", "", "")
    if (args.size == 5) {
      timeInterval = args(0)
      brokers = args(1)
      inputTopic = args(2)
      fromOffset = args(3)
      path2StoresReport = args(4)

    } else {
      timeInterval = "60"
      brokers = "54.83.9.85:9092"
      fromOffset = "smallest"
      inputTopic = "preseeds"
      path2StoresReport = "file:///home/ec2-user/storesReport/"

      conf.setMaster("local[*]")
    }
    val tmsp = (new java.util.Date).getTime
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
    try {
      var firstRun = true
      if (!firstRun) {
        ssc.stop(false)
      }
      firstRun = false
      // Create direct kafka stream with brokers and topics
      val topicsSet = inputTopic.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> fromOffset)
      val input = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc, kafkaParams, topicsSet)
      val parsed = Utils.parseBigMessage(input)

      val storesPerUser = parsed.map {
        case (msg, msgMap) =>
          msgCounter+=1
          val gglName = msgMap.apply("gglName")
          val userId = msgMap.apply("upc")
          (userId, gglName)
      }.groupByKey().map {
        case (userId, stores) =>
          storesPerUserCounter+=1
          val storeSet = stores.toSet.toList.sorted
          userId + "," + storeSet.mkString(",")
      }
      storesPerUser.foreachRDD { rdd => rdd.saveAsTextFile(path2StoresReport + "storesPerUser/" + tmsp) }
      
      val storesPerUserObj = ssc.sparkContext.textFile(path2StoresReport + "storesPerUser/" + tmsp, 1).collect().map { l =>
        val line = l.split(",").toList
        (line.head, line.tail)
      }.toList
      val storesBC = ssc.sparkContext.broadcast(storesPerUserObj.toMap)

      storesBC.value.foreach {
        case (user, compList) =>
          val grouped = parsed.map {
            case (msg, msgMap) =>
              //columns def
              val gglName = msgMap.apply("gglName")
              //rows def
              val details = {
                if (msgMap.apply("details").contains("Refurbished") || msgMap.apply("details").contains("Used"))
                  "Refurb"
                else
                  "New"
              }
              val ggId = msgMap.apply("ggId")
              val title = msgMap.apply("title")
              //data
              val url = msgMap.apply("url")
              val totalPrice = msgMap.apply("totalPrice")
              ((ggId, details), (gglName, title, url, totalPrice))
          }.filter {
            case ((ggId, details), (gglName, title, url, totalPrice)) =>
              (compList + user).contains(gglName)
          }.groupByKey().map {
            case ((ggId, details), l) =>
              val title = l.head._2
              val mapComp = l.map {
                case (gglName, title, url, totalPrice) =>
                  (gglName, (totalPrice, url))
              }.toMap
              val row = compList.map { comp => mapComp.getOrElse(comp, ("NA", "NA")) }.map { t => t._1 + "<<>>" + t._2 }

              (details + "," + title.replaceAll(",", "") + "," + row.mkString(","))
          }.foreachRDD { rdd => rdd.saveAsTextFile(path2StoresReport + "/" + user + tmsp) }
      }
    } catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}