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
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.spark.streaming.kafka.OffsetRange

/**
 * @author mike
 */

object Kafka2ProdXStoreReport {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    var (timeInterval, brokers, inputTopic, fromOffset, kafkaPartitions, path2StoresReport) = ("", "", "", "", "", "")
    if (args.size == 5) {
      timeInterval = args(0)
      brokers = args(1)
      inputTopic = args(2)
      fromOffset = args(3)
      kafkaPartitions = args(4)
      path2StoresReport = args(5)

    } else {
      timeInterval = "60"
      brokers = "54.83.9.85:9092"
      fromOffset = "smallest"
      kafkaPartitions = "100"
      inputTopic = "preseeds"
      path2StoresReport = "file:///home/ec2-user/storesReport/"

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
    val storesPerUserLength = ssc.sparkContext.accumulator(0L, "storesPerUserLength")
    try {

      // Create direct kafka stream with brokers and topics
      val topicsSet = inputTopic.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> fromOffset)

      //val offsetRanges:Array[OffsetRange] = (0 to (kafkaPartitions.toInt-1)).toArray.map{i=> OffsetRange.create(inputTopic, i, 0, 40000) } 
      //val inputr = KafkaUtils.createRDD[String, Array[Byte], StringDecoder, DefaultDecoder](sc, kafkaParams, offsetRanges)

      val input = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc, kafkaParams, topicsSet)
      val parsed = Utils.parseBigMessage(input).cache()

      val storesPerUser = parsed.map {
        case (msg, msgMap) =>
          msgCounter += 1
          val gglName = msgMap.apply("gglName")
          val userId = msgMap.apply("upc").trim()
          (userId, gglName)
      }.groupByKey().map {
        case (userId, stores) =>
          storesPerUserCounter += 1
          val storeSet = stores.toSet.toList.sorted
          val line = userId + "," + storeSet.mkString(",")
          storesPerUserLength += line.length()
          line
      }
      storesPerUser.transform { rdd => rdd.coalesce(1, false) }.saveAsTextFiles(path2StoresReport + "storesPerUser")
      
      val storesPerUserObj = ssc.sparkContext.textFile(path2StoresReport + "storesPerUser*", 1).collect().map { l =>
        val line = l.split(",").toList
        (line.head, line.tail)
      }.toList
      val storesBC = ssc.sparkContext.broadcast(storesPerUserObj)
      var iter = 0
      storesBC.value.foreach {
        case (user, compList) =>
          val groupedProdCounter = ssc.sparkContext.accumulator(0L, "groupedProdCounter_" + iter)
          val storeData = ssc.sparkContext.broadcast((user, compList))
          iter += 1
          val report = parsed.map {
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
              (storeData.value._2 + storeData.value._1).contains(gglName)
          }.groupByKey().map {
            case ((ggId, details), l) =>
              groupedProdCounter += 1
              val title = l.head._2
              val mapComp = l.map {
                case (gglName, title, url, totalPrice) =>
                  (gglName, (totalPrice, url))
              }.toMap
              val row = storeData.value._2.map { comp => mapComp.getOrElse(comp, ("NA", "NA")) }.map { t => t._1 + "<<>>" + t._2 }

              (details + "," + title.replaceAll(",", "") + "," + row.mkString(","))
          }
          //report.foreachRDD { rdd => rdd.coalesce(1, false).saveAsTextFile(path2StoresReport  + storeData.value._1 ) }
          report.transform { rdd => rdd.coalesce(1, false) }.saveAsTextFiles(path2StoresReport + storeData.value._1)
      }
    } catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(3500000)
    ssc.stop(false)
  }
}