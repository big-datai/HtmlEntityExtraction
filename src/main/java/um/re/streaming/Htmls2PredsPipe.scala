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
object Htmls2PredsPipe extends App {

  val conf = new SparkConf()
    .setAppName(getClass.getSimpleName)

  var inputMessagesCounter = 0L
  var parsedMessagesCounter = 0L
  var filteredMessagesCounter = 0L
  var candidatesMessagesCounter = 0L
  var predictionsMessagesCounter = 0L
  var outputMessagesCounter = 0L

  var missingModelCounter = 0L
  var patternFailedCounter = 0L
  var bothFailedCounter = 0L
  var allFalseCandidsCounter = 0L
  var modeledPatternEqualsCounter = 0L
  var modelPatternConflictCounter = 0L

  var exceptionCounter = 0L

  var (brokers, inputTopic, outputTopic, dMapPath, modelsPath ,statusFilters) = ("", "", "", "", "", "")
  if (args.size == 6) {
    brokers = args(0)
    inputTopic = args(1)
    outputTopic = args(2)
    dMapPath = args(3)
    modelsPath = args(4)
    statusFilters = args(5)
  } else {
    brokers = "localhost:9092"
    inputTopic = "htmls"
    outputTopic = "preds"
    dMapPath = "dMapNew"
    modelsPath = "/Users/dmitry/umbrella/Models/"
    statusFilters = "bothFailed"
    conf.setMaster("local[*]")
  }
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(5))

  val dMap = sc.broadcast(sc.textFile((dMapPath), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1))).toMap)
  try {
    // Create direct kafka stream with brokers and topics
    //TODO consider using multiple receivers for parallelism
    val topicsSet = inputTopic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val input = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)

    val parsed = Utils.parseMEnrichMessage(input)
    
    val candidates = parsed.transform(rdd => Utils.htmlsToCandidsPipe(rdd))

    val predictions = candidates.map {
      case (msg, candidList) =>
        try {
          val url = candidList.head.apply("url")
          val domain = Utils.getDomain(url)
          val domainCode = dMap.value.apply(domain)
          val (model, idf, selected_indices) = sc.objectFile[(GradientBoostedTreesModel, Array[Double], Array[Int])](modelsPath + domainCode + "/part-00000", 1).first

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
          case _: Exception => {
            val msgObj: MEnrichMessage = MEnrichMessage.string2Message(msg)
            msgObj.setModelPrice("-2")
            msgObj.sethtml("")
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
      (status,msgObj)
    }.cache
    
    messagesWithStatus.map(_._1).countByValue().foreachRDD { rdd =>
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
      }
    }
    val filteredMessages = messagesWithStatus.filter{case(status,msgObj) => !statusFilters.contains(status)}
    val output = filteredMessages.map{case(status,msgObj) => msgObj.toJson().toString().getBytes()}
    
    input.count().foreachRDD(rdd => { inputMessagesCounter += rdd.first() })
    parsed.count().foreachRDD(rdd => { parsedMessagesCounter += rdd.first() })
    candidates.count().foreachRDD(rdd => { candidatesMessagesCounter += rdd.first() })
    predictions.count().foreachRDD(rdd => { predictionsMessagesCounter += rdd.first() })
    filteredMessages.count().foreachRDD(rdd => { filteredMessagesCounter += rdd.first() })
    output.count().foreachRDD { rdd =>
      { outputMessagesCounter += rdd.first() }
      println("!@!@!@!@!   inputMessagesCounter " + inputMessagesCounter)
      println("!@!@!@!@!   parsedMessagesCounter " + parsedMessagesCounter)
      println("!@!@!@!@!   candidatesMessagesCounter " + candidatesMessagesCounter)
      println("!@!@!@!@!   predictionsMessagesCounter " + predictionsMessagesCounter)
      println("!@!@!@!@!   filteredMessagesCounter " + filteredMessagesCounter)
      println("!@!@!@!@!   outputMessagesCounter " + outputMessagesCounter)
      
      println("!@!@!@!@!   modeledPatternEqualsCounter " + modeledPatternEqualsCounter)
      println("!@!@!@!@!   modelPatternConflictCounter " + modelPatternConflictCounter)
      println("!@!@!@!@!   bothFailedCounter " + bothFailedCounter)
      println("!@!@!@!@!   patternFailedCounter " + patternFailedCounter)
      println("!@!@!@!@!   missingModelCounter " + missingModelCounter)
      println("!@!@!@!@!   allFalseCandidsCounter " + allFalseCandidsCounter)
      
      
      println("!@!@!@!@!   exceptionCounter " + exceptionCounter)
    }

    output.foreachRDD { rdd =>
      Utils.pushByteRDD2Kafka(rdd, outputTopic, brokers)
    }
  } catch {
    case e: Exception => {
      exceptionCounter += 1
      println("########  Somthing went wrong :( ")
      e.printStackTrace()
    }
  }
  ssc.start()
  ssc.awaitTermination()

}