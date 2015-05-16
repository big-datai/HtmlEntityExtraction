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
object HtmlsToPredictedPipe extends App {

  val sc = new SparkContext()
  val ssc = new StreamingContext(sc, Seconds(5))

  //TODO dmap for test
  //val dMap = sc.broadcast(sc.textFile("/Users/dmitry/umbrella/rawd/objects/dMap.txt", 1).collect().map(l => (l.split("\t")(0), l.split("\t")(1))).toMap)
  val dMap = sc.broadcast(sc.textFile((Utils.S3STORAGE + Utils.DMAP), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1))).toMap)
  
  val Array(brokers, inputTopic,outputTopic) = args
  //val Array(brokers, inputTopic, outputTopic) = Array("localhost:9092", "testOut", "test2")

  // Create direct kafka stream with brokers and topics
  //TODO consider using multiple receivers for parallelism
  val topicsSet = inputTopic.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
  val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
    ssc, kafkaParams, topicsSet)

  val parsed = messages.map { 
    case (s, msgBytes) =>
      val msg =  MEnrichMessage.string2Message(msgBytes)
      val msgEmptyHtml = msg //we remove the big html to keep the msg light
      msgEmptyHtml.sethtml("")
      val parsedMsg : (MEnrichMessage,Map[String,String]) = (msgEmptyHtml,Utils.json2Map(Json.parse(msg.toJson().toString())))
      parsedMsg
  }
  
  val candidates = parsed.transform(rdd => Utils.htmlsToCandidsPipe(rdd))
  
  val predictions = candidates.map{case(msg,candidList) =>
	  //TODO test path
    val url = candidList.head.apply("url")
    val domain = Utils.getDomain(url)
    val domainCode = dMap.value.apply(domain)  
    //val (model, idf, selected_indices) = sc.objectFile[(GradientBoostedTreesModel, Array[Double], Array[Int])]("/Users/dmitry/umbrella/rawd/objects/Models/" + domainCode + "/part-00000", 1).first
    val (model, idf, selected_indices) = sc.objectFile[(GradientBoostedTreesModel,Array[Double],Array[Int])](Utils.S3STORAGE + Utils.MODELS + domainCode+"/part-00000",1).first

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
      var selectedCandid = (0.0,0.0,"0")
      if (modelPredictions.filter(c => c._2 == 1).size >=1)
    	  selectedCandid = modelPredictions.filter(c => c._2 == 1).sorted.reverse.head
    	  else
    	    selectedCandid = (0,0,"-1")

      val predictedPrice = selectedCandid._3
      msg.setModelPrice(predictedPrice)
      msg
  }

  predictions.foreachRDD { rdd =>
    rdd.foreachPartition { p =>
      val props = new Properties()
      props.put("metadata.broker.list", brokers)
      props.put("serializer.class", "kafka.serializer.DefaultEncoder")

      @transient val config = new ProducerConfig(props)
      @transient val producer = new Producer[String, Array[Byte]](config)
      p.foreach(rec => producer.send(new KeyedMessage[String, Array[Byte]](outputTopic, rec.toJson().toString().getBytes())))
      producer.close()
    }
  }

  ssc.start()
  ssc.awaitTermination()

}