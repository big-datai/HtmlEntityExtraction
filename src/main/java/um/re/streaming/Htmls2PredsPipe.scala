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
import com.utils.aws.AWSUtils
object Htmls2PredsPipe {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    var (brokers, inputTopic, outputTopic, logTopic, dMapPath, modelsPath, statusFilters) = ("", "", "", "", "", "", "")
    if (args.size == 7) {
      brokers = args(0)
      inputTopic = args(1)
      outputTopic = args(2)
      logTopic = args(3)
      dMapPath = args(4)
      modelsPath = args(5)
      statusFilters = args(6)

    } else {
      //by default all in root folder of hdfs
      brokers = "54.83.9.85:9092"
      inputTopic = "htmls"
      outputTopic = "preds"
      logTopic = "logs"
      dMapPath = "/dMapNew"
      modelsPath = "/Models/"
      statusFilters = "bothFailed"
      conf.setMaster("yarn-client")
    }
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    brokers = AWSUtils.getPrivateIp(brokers.substring(0, brokers.length() - 5)) + ":9092"

    //counters and accumulators
    val inputMessagesCounter = ssc.sparkContext.accumulator(0L)
    val parsedMessagesCounter = ssc.sparkContext.accumulator(0L)
    val candidatesMessagesCounter = ssc.sparkContext.accumulator(0L)
    val predictionsMessagesCounter = ssc.sparkContext.accumulator(0L)
    val outputMessagesCounter = ssc.sparkContext.accumulator(0L)

    val missingModelCounter = ssc.sparkContext.accumulator(0L)
    val patternFailedCounter = ssc.sparkContext.accumulator(0L)
    val bothFailedCounter = ssc.sparkContext.accumulator(0L)
    val allFalseCandidsCounter = ssc.sparkContext.accumulator(0L)
    val modeledPatternEqualsCounter = ssc.sparkContext.accumulator(0L)
    val modelPatternConflictCounter = ssc.sparkContext.accumulator(0L)

    var exceptionCounter = 0L

    //Broadcast variables
    val hash = new scala.collection.immutable.HashMap
    val dHashMap = hash ++ ssc.sparkContext.textFile((dMapPath), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1))).toMap
    val dMapBC = ssc.sparkContext.broadcast(dHashMap)

    val modelsMap: Map[String, (GradientBoostedTreesModel, Array[Double], Array[Int])] = dHashMap.values.map { domainCode =>
      val (model, idf, selected_indices) = sc.objectFile[(GradientBoostedTreesModel, Array[Double], Array[Int])](modelsPath + domainCode + "/part-00000", 1).first
      (domainCode, (model, idf, selected_indices))
    }.toMap
    val modelsHashMap = hash ++ modelsMap
    val modelsBC = ssc.sparkContext.broadcast(modelsHashMap)

    try {
      // Create direct kafka stream with brokers and topics
      //TODO consider using multiple receivers for parallelism
      val topicsSet = inputTopic.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "largest")
      val input = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc, kafkaParams, topicsSet)

      val parsed = Utils.parseMEnrichMessage(input)

      val candidates = parsed.transform(rdd => Utils.htmlsToCandidsPipe(rdd))

      val predictions = candidates.map {
        case (msg, candidList) =>
          try {
            val url = candidList.head.apply("url")
            val domain = Utils.getDomain(url)
            val domainCode = dMapBC.value.apply(domain)
            val (model, idf, selected_indices) = modelsBC.value.apply(domainCode)

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
              msgObj.setM_issue("ERROR")
              msgObj
            }
          }
      }

      val messagesWithStatus = predictions.map { msgObj =>
        val modelPrice = Utils.parseDouble(msgObj.getModelPrice())
        val updatedPrice = Utils.parseDouble(msgObj.getUpdatedPrice())
        var status = ""
        var allFalseCandids = false
        var missingModel = false
        var patternFailed = false
        var modeledPatternEquals = false

        if (modelPrice.get == -1.0)
          allFalseCandids = true
        if (modelPrice.get == -2.0)
          missingModel = true
        if (updatedPrice.get.toInt == 0)
          patternFailed = true
        if (!patternFailed && !missingModel && !allFalseCandids && ((modelPrice.get - updatedPrice.get) < 0.009))
          modeledPatternEquals = true

        if (modeledPatternEquals)
          status = "modeledPatternEquals"
        else if (!allFalseCandids && !missingModel && !patternFailed)
          status = "modelPatternConflict"
        else if ((allFalseCandids || missingModel) && patternFailed)
          status = "bothFailed"
        else if (patternFailed)
          status = "patternFailed"
        else if (missingModel)
          status = "missingModel"
        else if (allFalseCandids)
          status = "allFalseCandids"
        if (statusFilters.contains(status)) {
          msgObj.setM_errorLocation("Package: " + this.getClass.getPackage.getName + " Name: " + this.getClass.getName + " Step: statusing")
          msgObj.setM_errorMessage(status)
        }
        (status, msgObj)
      }.cache

      /*messagesWithStatus.map(_._1).countByValue().foreachRDD { rdd =>
        rdd.foreach {
          case (counterType, count) =>
            counterType match {
              case "modeledPatternEquals" => modeledPatternEqualsCounter += count
              case "modelPatternConflict" => modelPatternConflictCounter += count
              case "bothFailed"           => bothFailedCounter += count
              case "patternFailed"        => patternFailedCounter += count
              case "missingModel"         => missingModelCounter += count
              case "allFalseCandids"      => allFalseCandidsCounter += count
            }
        }*/

      val filteredMessages = messagesWithStatus.filter { case (status, msgObj) => !statusFilters.contains(status) }
      val output = filteredMessages.map { case (status, msgObj) => msgObj.toJson().toString().getBytes() }

      /*input.count().foreachRDD(rdd => { inputMessagesCounter += rdd.first() })
      parsed.count().foreachRDD(rdd => { parsedMessagesCounter += rdd.first() })
      candidates.count().foreachRDD(rdd => { candidatesMessagesCounter += rdd.first() })
      predictions.count().foreachRDD(rdd => { predictionsMessagesCounter += rdd.first() })
      output.count().foreachRDD { rdd =>
        { outputMessagesCounter += rdd.first() }
        println("!@!@!@!@!   inputMessagesCounter " + inputMessagesCounter)
        println("!@!@!@!@!   parsedMessagesCounter " + parsedMessagesCounter)
        println("!@!@!@!@!   candidatesMessagesCounter " + candidatesMessagesCounter)
        println("!@!@!@!@!   predictionsMessagesCounter " + predictionsMessagesCounter)
        println("!@!@!@!@!   outputMessagesCounter " + outputMessagesCounter)

        println("!@!@!@!@!   modeledPatternEqualsCounter " + modeledPatternEqualsCounter.value)
        println("!@!@!@!@!   modelPatternConflictCounter " + modelPatternConflictCounter.value)
        println("!@!@!@!@!   bothFailedCounter " + bothFailedCounter.value)
        println("!@!@!@!@!   patternFailedCounter " + patternFailedCounter.value)
        println("!@!@!@!@!   missingModelCounter " + missingModelCounter.value)
        println("!@!@!@!@!   allFalseCandidsCounter " + allFalseCandidsCounter.value)

        println("!@!@!@!@!   exceptionCounter " + exceptionCounter)
      }*/

      output.foreachRDD { rdd =>
        Utils.pushByteRDD2Kafka(rdd, outputTopic, brokers, logTopic)
      }
    } catch {
      case e: Exception => {
        exceptionCounter += 1
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionLocalizedMessage : " + e.getLocalizedMessage +
          "\n#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}