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

object PredictionPipeline extends App {
  val DELIMITER = "!#@#@!"

  val sc = new SparkContext()
  val ssc = new StreamingContext(sc, Seconds(5))
  
  val dMap = sc.broadcast(sc.textFile((Utils.S3STORAGE + Utils.DMODELS + "part-00000"), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1))).toMap)
  val nf = um.re.utils.PriceParcer
  nf.snippetSize = 150
  val candidFinder = sc.broadcast(nf)
  
  val Array(brokers, inputTopic,outputTopic) = args
  //val Array(brokers, inputTopic,outputTopic) = Array("localhost:9092", "test2","testOut")

  // Create direct kafka stream with brokers and topics
  //TODO consider using multiple receivers for parallelism
  val topicsSet = inputTopic.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
  val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, topicsSet)

  val parsed = messages.map { msg =>
    val arr = msg._2.split(DELIMITER)
    //TODO maybe use some msg class rather than concatenated string
    if (arr.size != 2) 
      //TODO log bad input 
      ""
    else{
      val url = arr(0)
      val html = arr(1)
      val domain = Utils.getDomain(arr(0))
      val domainCode = dMap.value.apply(domain)
      //TODO test path
      val (model, idf, selected_indices) = sc.objectFile[(GradientBoostedTreesModel,Array[Double],Array[Int])](Utils.HDFSSTORAGE + "/temp" + Utils.DMODELS + domainCode+"*",1).first
      
      val candidates = candidFinder.value.findM(url, html).map{candid =>
        val price = candid.apply("priceCandidate")
        val location = Integer.valueOf(candid.apply("location")).toDouble
        val text_before = Utils.tokenazer(candid.apply("text_before"))
        val text_after = Utils.tokenazer(candid.apply("text_after"))
        val text = text_before++text_after
        val length = html.length().toDouble
        
        val hashingTF = new HashingTF(1000)
        val tf = Transformer.projectByIndices(hashingTF.transform(text).toArray, selected_indices)
        val tfidf = (tf,idf).zipped.map((tf,idf)=>tf*idf)
        
        val features = tfidf ++ Array(location/length)
        val values = features.filter { l => l != 0 }
        val index = features.zipWithIndex.filter { l => l._1 != 0 }.map { l => l._2 }
        val featureVec = Vectors.sparse(features.length, index, values)
        
        val prediction = model.predict(featureVec)
        val confidence = Transformer.confidenceGBT(model,featureVec)
        (confidence,prediction,price)
        }
      val selectedCandid = candidates.filter(c=> c._2==1).sorted.reverse.head
      //TODO deal with extreme case e.g. all candidates are negative
      val predictedPrice = selectedCandid._3 
      Array(url,predictedPrice).mkString(DELIMITER)
      
    }
  }.filter(t=> t.equals(""))

  //  def toKMsg(v:String) = { new KeyedMessage[String, String]("testOut", v)}

  parsed.foreachRDD { rdd =>
    rdd.foreachPartition { p =>
      val props = new Properties()
      props.put("metadata.broker.list", brokers)
      props.put("serializer.class", "kafka.serializer.StringEncoder")

      @transient val config = new ProducerConfig(props)
      @transient val producer = new Producer[String, String](config)
      p.foreach(rec => producer.send(new KeyedMessage[String, String](outputTopic, rec)))
      producer.close()
    }
  }

  ssc.start()
  ssc.awaitTermination()

}