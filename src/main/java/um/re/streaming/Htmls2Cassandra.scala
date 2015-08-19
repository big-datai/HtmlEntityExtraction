package um.re.streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import kafka.producer._
import org.apache.spark.streaming.Seconds
import java.util.Properties
import um.re.utils.Utils
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.rdd.RDD
import um.re.transform.Transformer
import org.apache.spark.mllib.linalg.Vectors
import kafka.serializer.DefaultDecoder
import com.utils.messages.MEnrichMessage
import play.api.libs.json.Json
import org.apache.spark._
import org.joda.time.DateTime
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.SomeColumns
import scala.collection.immutable.HashMap
import com.utils.aws.AWSUtils

object Htmls2Cassandra {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    var (timeInterval, brokers, inputTopic, fromOffset, modelsPath, logStatusFilters, cassandraHost, keySpace, tableRT, tableH, tableCL, dbStatusFilters, stopMessagesThreshold) = ("", "", "", "", "", "", "", "", "", "", "", "", "")
    if (args.size == 12) {
      timeInterval = args(0)
      brokers = args(1)
      inputTopic = args(2)
      fromOffset = args(3)
      modelsPath = args(4)
      logStatusFilters = args(5)
      cassandraHost = args(6)
      keySpace = args(7)
      tableRT = args(8)
      tableH = args(9)
      tableCL = args(10)
      dbStatusFilters = args(11)
      //stopMessagesThreshold = args(11)
    } else {
      timeInterval = "5"
      brokers = "localhost:9092"
      fromOffset = "smallest"
      inputTopic = "htmls"
      modelsPath = "/Users/mike/umbrella/ModelsObject/"
      logStatusFilters = "bothFailed,minorModelPatternConflict,majorModelPatternConflict,patternFailed,missingModel,allFalseCandids"
      cassandraHost = "localhost"
      keySpace = "demo"
      tableRT = "real_time_market_prices"
      tableH = "historical_prices"
      tableCL = "core_logs"
      dbStatusFilters = "modeledPatternEquals,minorModelPatternConflict,majorModelPatternConflict,patternFailed,missingModel,allFalseCandids"
      //stopMessagesThreshold = "50" //"5000000"
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
    try {
      val innerCassandraHost = AWSUtils.getPrivateIp(cassandraHost)
      cassandraHost = innerCassandraHost
    } catch {
      case e: Exception => {
        println("#?#?#?#?#?#?#  Couldn't get inner Cassandra IP, using : " + cassandraHost +
          "\n#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }

    conf.set("spark.cassandra.connection.host", cassandraHost)

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(timeInterval.toInt))

    //counters and accumulators
    //TODO update input and parsed counters
    //val inputMessagesCounter = ssc.sparkContext.accumulator(0L)
    //val parsedMessagesCounter = ssc.sparkContext.accumulator(0L)
    val candidatesMessagesCounter = ssc.sparkContext.accumulator(0L)
    val predictionsMessagesCounter = ssc.sparkContext.accumulator(0L)
    val filteredMessagesCounter = ssc.sparkContext.accumulator(0L)
    val loggedMessagesCounter = ssc.sparkContext.accumulator(0L)
    val historicalFeedCounter = ssc.sparkContext.accumulator(0L)
    val realTimeFeedCounter = ssc.sparkContext.accumulator(0L)

    val missingModelCounter = ssc.sparkContext.accumulator(0L)
    val patternFailedCounter = ssc.sparkContext.accumulator(0L)
    val bothFailedCounter = ssc.sparkContext.accumulator(0L)
    val allFalseCandidsCounter = ssc.sparkContext.accumulator(0L)
    val modeledPatternEqualsCounter = ssc.sparkContext.accumulator(0L)
    val modelPatternConflictCounter = ssc.sparkContext.accumulator(0L)
    val minorModelPatternConflictCounter = ssc.sparkContext.accumulator(0L)
    val majorModelPatternConflictCounter = ssc.sparkContext.accumulator(0L)

    var exceptionCounter = 0L

    //Broadcast variables

    val modelsHashMap = ssc.sparkContext.objectFile[HashMap[String, (GradientBoostedTreesModel, Array[Double], Array[Int])]](modelsPath, 1).first
    val modelsBC = ssc.sparkContext.broadcast(modelsHashMap)

    try {
      // Create direct kafka stream with brokers and topics
      //TODO consider using createKafkaaStream which uses the high level consumer API
      val topicsSet = inputTopic.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> fromOffset)
      val input = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc, kafkaParams, topicsSet)
      val parsed = Utils.parseMEnrichMessage(input)

      val candidates = parsed.transform(rdd => Utils.htmlsToCandidsPipe(rdd))

      val predictions = candidates.map {
        case (msg, candidList) =>
          try {
            //Update candidates accumulator 
            candidatesMessagesCounter += 1

            val url = candidList.head.apply("url")
            val domain = Utils.getDomain(url)
            val (model, idf, selected_indices) = modelsBC.value.apply(domain)

            val modelPredictions = candidList.map { candid =>
              val priceCandidate = candid.apply("priceCandidate")
              val location = Integer.valueOf(candid.apply("location")).toDouble
              val text_before = Utils.tokenazer(candid.apply("text_before"))
              val text_after = Utils.tokenazer(candid.apply("text_after"))
              val text = text_before ++ text_after

              val hashingTF = new HashingTF(1000)
              val tf = Transformer.projectByIndices(hashingTF.transform(text).toArray, selected_indices)
              val tfidf = (tf, idf).zipped.map((tf, idf) => tf * idf)

              val features = tfidf ++ Array(location)
              val values = features.filter { l => l != 0 }
              val index = features.zipWithIndex.filter { l => l._1 != 0 }.map { l => l._2 }
              val featureVec = Vectors.sparse(features.length, index, values)

              val prediction = model.predict(featureVec)
              val confidence = Transformer.confidenceGBT(model, featureVec)
              (confidence, prediction, priceCandidate)
            }
            //TODO deal with extreme case e.g. all candidates are negative
            var selectedCandid = (0.0, 0.0, "0")
            if (modelPredictions.filter(c => c._2 == 1).size >= 1)
              selectedCandid = modelPredictions.filter(c => c._2 == 1).sorted.reverse.head
            else
              selectedCandid = (0, 0, "-1")

            val predictedPrice = selectedCandid._3
            val msgObj: MEnrichMessage = MEnrichMessage.string2Message(msg)
            msgObj.setModelPrice(predictedPrice)
            msgObj.sethtml("")
            msgObj
          } catch {
            //TODO better log exceptions
            case e: Exception => {
              val msgObj: MEnrichMessage = MEnrichMessage.string2Message(msg)
              msgObj.setModelPrice("-2")
              msgObj.sethtml("")
              msgObj.setM_exception(e.getMessage)
              msgObj.setM_stackTrace(e.getStackTraceString)
              msgObj.setM_errorLocation("Package: " + this.getClass.getPackage.getName + " Name: " + this.getClass.getName + " Step: predictions")
              msgObj
            }
          }
      }

      val messagesWithStatus = predictions.map { msgObj =>
        //Update predictions accumulator
        predictionsMessagesCounter += 1

        val modelPrice = Utils.parseDouble(msgObj.getModelPrice()).get
        val updatedPrice = Utils.parseDouble(msgObj.getUpdatedPrice()).get
        var status = ""
        var allFalseCandids = false
        var missingModel = false
        var patternFailed = false
        var modeledPatternEquals = false

        //raise flags for status logic
        if (modelPrice == -1.0)
          allFalseCandids = true
        if (modelPrice == -2.0)
          missingModel = true
        if (updatedPrice.toInt == 0)
          patternFailed = true
        if (!patternFailed && !missingModel && !allFalseCandids && ((modelPrice - updatedPrice).abs < 0.009))
          modeledPatternEquals = true

        //Set status and update their accumulators
        if (modeledPatternEquals) {
          status = "modeledPatternEquals"
          modeledPatternEqualsCounter += 1
        } else if (!allFalseCandids && !missingModel && !patternFailed) {
          if ((updatedPrice - modelPrice).abs / math.max(updatedPrice, modelPrice) <= 0.1) {
            status = "minorModelPatternConflict"
            minorModelPatternConflictCounter += 1
          } else {
            status = "majorModelPatternConflict"
            majorModelPatternConflictCounter += 1
          }
          modelPatternConflictCounter += 1
        } else if ((allFalseCandids || missingModel) && patternFailed) {
          status = "bothFailed"
          bothFailedCounter += 1
        } else if (patternFailed) {
          status = "patternFailed"
          patternFailedCounter += 1
        } else if (missingModel) {
          status = "missingModel"
          missingModelCounter += 1
        } else if (allFalseCandids) {
          status = "allFalseCandids"
          allFalseCandidsCounter += 1
        }
        msgObj.setM_issue(status)
        if (logStatusFilters.contains(status)) {
          msgObj.setM_errorLocation("Package: " + this.getClass.getPackage.getName + " Name: " + this.getClass.getName + " Step: statusing")
          msgObj.setM_errorMessage(status)
          loggedMessagesCounter += 1
        }
        if (dbStatusFilters.contains(status))
          filteredMessagesCounter += 1
        (status, msgObj.toJson().toString().getBytes)
      }.cache

      val historicalFeed = Utils.parseMEnrichMessage(messagesWithStatus.filter { case (status, msg) => dbStatusFilters.contains(status) }).map {
        case (msg, msgMap) =>
          //yyyy-mm-dd'T'HH:mm:ssZ  2015-07-15T16:25:52.325Z
          val date = DateTime.parse(msgMap.apply("lastUpdatedTime")).toDate() //,DateTimeFormat.forPattern("yyyy-mm-dd'T'HH:mm:ssZ"));
          val row = (msgMap.apply("prodId"), msgMap.apply("domain"), date, Utils.getPriceFromMsgMap(msgMap), msgMap.apply("title"), msgMap.apply("url"))
          historicalFeedCounter += 1
          row
      }.cache

      historicalFeed.map { l => (l._1, l._2, l._3, l._4, l._5) }.saveToCassandra(keySpace, tableH, SomeColumns("sys_prod_id", "store_id", "tmsp", "price", "sys_prod_title"))

      val realTimeFeed = historicalFeed.map { t =>
        val row = (t._1, t._2, t._4, t._5,t._6)
        realTimeFeedCounter += 1
        row
      }
      realTimeFeed.saveToCassandra(keySpace, tableRT, SomeColumns("sys_prod_id", "store_id", "price", "sys_prod_title", "url"))

      val coreLogs = Utils.parseMEnrichMessage(messagesWithStatus.filter { case (status, msg) => logStatusFilters.contains(status) }).map {
        case (msg, msgMap) =>
          //yyyy-mm-dd'T'HH:mm:ssZ  2015-07-15T16:25:52.325Z
          val date = DateTime.parse(msgMap.apply("lastUpdatedTime")).toDate() //,DateTimeFormat.forPattern("yyyy-mm-dd'T'HH:mm:ssZ"));
          val row = (date, msgMap.apply("errorMessage"), msgMap.apply("url"), msgMap.apply("patternsText"), msgMap.apply("domain"), msgMap.apply("price"), msgMap.apply("updatedPrice"), msgMap.apply("exception"), msgMap.apply("modelPrice"), msgMap.apply("stackTrace"), msgMap.apply("issue"), msgMap.apply("patternsHtml"), msgMap.apply("prodId"), msgMap.apply("lastScrapedTime"), msgMap.apply("errorLocation"), msgMap.apply("title"), msgMap.apply("html"), msgMap.apply("shipping"), Utils.getPriceFromMsgMap(msgMap))
          row
      }.cache
      coreLogs.saveToCassandra(keySpace, tableCL, SomeColumns("lastupdatedtime", "errormessage", "url", "patternstext", "domain", "price", "updatedprice", "exception", "modelprice", "stacktrace", "issue", "patternshtml", "prodid", "lastscrapedtime", "errorlocation", "title", "html", "shipping", "selectedprice"))
      messagesWithStatus
        .filter { case (status, msg) => logStatusFilters.contains(status) }.map { case (status, msg) => msg }
        .foreachRDD { rdd =>
          //Utils.pushByteRDD2Kafka(rdd, "", brokers, logTopic)
          //println("!@!@!@!@!   inputMessagesCounter " + inputMessagesCounter)
          //println("!@!@!@!@!   parsedMessagesCounter " + parsedMessagesCounter)
          println("!@!@!@!@!   candidatesMessagesCounter " + candidatesMessagesCounter)
          println("!@!@!@!@!   predictionsMessagesCounter " + predictionsMessagesCounter)
          println("!@!@!@!@!   filteredMessagesCounter " + filteredMessagesCounter)
          println("!@!@!@!@!   historicalFeedCounter " + historicalFeedCounter)
          println("!@!@!@!@!   realTimeFeedCounter " + realTimeFeedCounter)
          println("!@!@!@!@!   loggedMessagesCounter " + loggedMessagesCounter)

          println("!@!@!@!@!   modeledPatternEqualsCounter " + modeledPatternEqualsCounter.value)
          println("!@!@!@!@!   minorModelPatternConflictCounter " + minorModelPatternConflictCounter.value)
          println("!@!@!@!@!   majorModelPatternConflictCounter " + majorModelPatternConflictCounter.value)
          println("!@!@!@!@!   modelPatternConflictCounter " + modelPatternConflictCounter.value)
          println("!@!@!@!@!   bothFailedCounter " + bothFailedCounter.value)
          println("!@!@!@!@!   patternFailedCounter " + patternFailedCounter.value)
          println("!@!@!@!@!   missingModelCounter " + missingModelCounter.value)
          println("!@!@!@!@!   allFalseCandidsCounter " + allFalseCandidsCounter.value)

          println("!@!@!@!@!   exceptionCounter " + exceptionCounter)
        }
      /*
      messagesWithStatus.foreachRDD{rdd =>
        // check stop condition
          if (candidatesMessagesCounter.value >= stopMessagesThreshold.toLong) {
            println("~!~!~!~!~ Reached messages stop threshold , threshold is : " + stopMessagesThreshold + " , candidatesMessagesCounter value : " + candidatesMessagesCounter.value +
              "\n~!~!~!~!~ Shutting Down...")
            ssc.stop(true,true)
          }
        }*/

    } catch {
      case e: Exception => {
        exceptionCounter += 1
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}