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
object Kafka2ProdXStoreReport {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    var (timeInterval, brokers, inputTopic, fromOffset, inputPath, outputPath) = ("", "", "", "", "", "")
    if (args.size == 6) {
      timeInterval = args(0)
      brokers = args(1)
      inputTopic = args(2)
      fromOffset = args(3)
      inputPath = args(4)
      outputPath = args(5)

    } else {
      timeInterval = "60"
      brokers = "54.225.122.3:9092"
      fromOffset = "smallest"
      inputPath = "file:///home/ec2-user/storesReport/CompPerUser/output*"
      inputTopic = "preseeds"
      outputPath = "file:///home/ec2-user/storesReport/UserReports/"

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
    try {

      // Create direct kafka stream with brokers and topics
      val topicsSet = inputTopic.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> fromOffset)

      val input = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc, kafkaParams, topicsSet)
      val parsed = Utils.parseBigMessage(input).cache()

      val storesPerUserObj = ssc.sparkContext.textFile(inputPath, 1).collect().map { l =>
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
                if (msgMap.apply("details").contains("Refurbished"))
                  "Refurb"
                else if (msgMap.apply("details").contains("Used"))
                  "Used"
                else
                  "New"
              }
              val ggId = msgMap.apply("ggId")
              val title = msgMap.apply("title").replaceAll(",", "")
              val sku = msgMap.apply("sku").replaceAll(",", "")
              //data
              val url = msgMap.apply("url")
              val totalPrice = {
                if (msgMap.apply("totalPrice").length > 0)
                  msgMap.apply("totalPrice")
                else
                  msgMap.apply("price")
              }
              ((ggId, details), (gglName, title, url, totalPrice, sku))
          }.filter {
            case ((ggId, details), (gglName, title, url, totalPrice, sku)) =>
              (storeData.value._2 + storeData.value._1).contains(gglName)
          }.groupByKey().filter {
            case ((ggId, details), l) =>
              l.map(_._1).toList.contains(storeData.value._1)
          }.map {
            case ((ggId, details), l) =>
              groupedProdCounter += 1
              val sku = l.head._5
              val title = l.head._2
              val mapComp = l.map {
                case (gglName, title, url, totalPrice, sku) =>
                  (gglName, (totalPrice, url))
              }.toMap
              val row = storeData.value._2.map { comp => mapComp.getOrElse(comp, ("NA", "NA")) }.map { t => t._1 + "<<>>" + t._2 }

              (details + "," + sku + "," + title.replaceAll(",", "") + "," + row.mkString(","))
          }
          //report.transform { rdd => rdd.coalesce(1, false) }.saveAsTextFiles(outputPath +"perUserRep/"+ storeData.value._1)
          report.transform { rdd =>
            val header = ssc.sparkContext.parallelize(Array("Condition,SKU,Title," + storeData.value._2.mkString(",")), 1)
            val onePartRdd = rdd.coalesce(1, false)
            header.union(onePartRdd).coalesce(1, false)
          }.saveAsTextFiles(outputPath + storeData.value._1)
      }
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
