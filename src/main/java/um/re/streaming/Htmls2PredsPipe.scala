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
import com.utils.messages.BigMessage
import play.api.libs.json.Json
import org.apache.spark._
import scala.collection.immutable.HashMap
import com.utils.aws.AWSUtils

object Htmls2PredsPipe {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    var (timeInterval, brokers, inputTopic, fromOffset, outputTopic, logTopic, modelsPath, statusFilters) = ("", "", "", "", "", "", "", "")
    if (args.size == 8) {
      timeInterval = args(0)
      brokers = args(1)
      inputTopic = args(2)
      fromOffset = args(3)
      outputTopic = args(4)
      logTopic = args(5)
      modelsPath = args(6)
      statusFilters = args(7)
    } else {
      //by default all in root folder of hdfs
      timeInterval = "2"
      brokers = "54.225.122.3:9092"
      fromOffset = "smallest"
      inputTopic = "htmls"
      outputTopic = "preds"
      logTopic = "sparkLogs"
      modelsPath = "/ModelsObject/"
      statusFilters = "modeledPatternEquals"+",modelPatternConflict,patternFailed,missingModel,allFalseCandids"
      conf.setMaster("yarn-client") /*
      timeInterval = "20"
      brokers = "localhost:9092"
      fromOffset = "smallest"
      inputTopic = "htmls"
      outputTopic = "preds"
      logTopic = "logs"
      modelsPath = "/Users/mike/umbrella/ModelsObject/"
      statusFilters = "modeledPatternEquals,modelPatternConflict,patternFailed,missingModel,allFalseCandids"
      conf.setMaster("local[*]")*/
    }
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
    val ssc = new StreamingContext(sc, Seconds(timeInterval.toInt))

    //counters and accumulators
    //TODO update input and parsed counters
    //val inputMessagesCounter = ssc.sparkContext.accumulator(0L)
    //val parsedMessagesCounter = ssc.sparkContext.accumulator(0L)
    val candidatesMessagesCounter = ssc.sparkContext.accumulator(0L)
    val predictionsMessagesCounter = ssc.sparkContext.accumulator(0L)
    val filteredMessagesCounter = ssc.sparkContext.accumulator(0L)
    val loggedMessagesCounter = ssc.sparkContext.accumulator(0L)

    val missingModelCounter = ssc.sparkContext.accumulator(0L)
    val patternFailedCounter = ssc.sparkContext.accumulator(0L)
    val bothFailedCounter = ssc.sparkContext.accumulator(0L)
    val allFalseCandidsCounter = ssc.sparkContext.accumulator(0L)
    val modeledPatternEqualsCounter = ssc.sparkContext.accumulator(0L)
    val modelPatternConflictCounter = ssc.sparkContext.accumulator(0L)

    var exceptionCounter = 0L

    //Broadcast variables
    val modelsHashMap = ssc.sparkContext.objectFile[HashMap[String,(GradientBoostedTreesModel, Array[Double], Array[Int])]](modelsPath, 1).first
    val modelsBC = ssc.sparkContext.broadcast(modelsHashMap)

    try {
      // Create direct kafka stream with brokers and topics
      //TODO consider using createKafkaaStream which uses the high level consumer API
      val topicsSet = inputTopic.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> fromOffset)
      val input = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc, kafkaParams, topicsSet)

      val parsed = Utils.parseBigMessage(input)

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
            val msgObj: BigMessage = BigMessage.string2Message(msg)
            msgObj.setModelPrice(predictedPrice)
            msgObj.sethtml("")
            msgObj
          } catch {
            //TODO better log exceptions
            case e: Exception => {
              val msgObj: BigMessage = BigMessage.string2Message(msg)
              msgObj.setModelPrice("-2")
              msgObj.sethtml("")
              msgObj.setException(e.getMessage)
              msgObj.setStackTrace(e.getStackTraceString)
              msgObj.setErrorLocation("Package: " + this.getClass.getPackage.getName + " Name: " + this.getClass.getName + " Step: predictions")
              msgObj.setIssue("ERROR")
              msgObj
            }
          }
      }

      val messagesWithStatus = predictions.map { msgObj =>
        //Update predictions accumulator
        predictionsMessagesCounter += 1

        val modelPrice = Utils.parseDouble(msgObj.getModelPrice())
        val updatedPrice = Utils.parseDouble(msgObj.getUpdatedPrice())
        var status = ""
        var allFalseCandids = false
        var missingModel = false
        var patternFailed = false
        var modeledPatternEquals = false

        //raise flags for status logic
        if (modelPrice.get == -1.0)
          allFalseCandids = true
        if (modelPrice.get == -2.0)
          missingModel = true
        if (updatedPrice.get.toInt == 0)
          patternFailed = true
        if (!patternFailed && !missingModel && !allFalseCandids && ((modelPrice.get - updatedPrice.get) < 0.009))
          modeledPatternEquals = true

        //Set status and update their accumulators
        if (modeledPatternEquals) {
          status = "modeledPatternEquals"
          modeledPatternEqualsCounter += 1
        } else if (!allFalseCandids && !missingModel && !patternFailed) {
          status = "modelPatternConflict"
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
        if (!statusFilters.contains(status)) {
          msgObj.setErrorLocation("Package: " + this.getClass.getPackage.getName + " Name: " + this.getClass.getName + " Step: statusing")
          msgObj.setErrorMessage(status)
          loggedMessagesCounter += 1
        } else
          filteredMessagesCounter += 1
        msgObj.toJson().toString().getBytes
      }

      messagesWithStatus.foreachRDD { rdd =>
        Utils.pushByteRDD2Kafka(rdd, outputTopic, brokers, logTopic)
        //println("!@!@!@!@!   inputMessagesCounter " + inputMessagesCounter)
        //println("!@!@!@!@!   parsedMessagesCounter " + parsedMessagesCounter)
        println("!@!@!@!@!   candidatesMessagesCounter " + candidatesMessagesCounter)
        println("!@!@!@!@!   predictionsMessagesCounter " + predictionsMessagesCounter)
        println("!@!@!@!@!   filteredMessagesCounter " + filteredMessagesCounter)
        println("!@!@!@!@!   loggedMessagesCounter " + loggedMessagesCounter)

        println("!@!@!@!@!   modeledPatternEqualsCounter " + modeledPatternEqualsCounter.value)
        println("!@!@!@!@!   modelPatternConflictCounter " + modelPatternConflictCounter.value)
        println("!@!@!@!@!   bothFailedCounter " + bothFailedCounter.value)
        println("!@!@!@!@!   patternFailedCounter " + patternFailedCounter.value)
        println("!@!@!@!@!   missingModelCounter " + missingModelCounter.value)
        println("!@!@!@!@!   allFalseCandidsCounter " + allFalseCandidsCounter.value)

        println("!@!@!@!@!   exceptionCounter " + exceptionCounter)
      }

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