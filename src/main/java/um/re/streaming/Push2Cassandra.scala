package um.re.streaming

import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
import org.apache.spark.{ Logging, SparkContext, SparkConf }
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import um.re.utils.Utils

object Push2Cassandra extends App {
  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    
  var (brokers, cassandraHost, inputTopic, keySpace, tableRT, tableH) = ("", "", "", "", "", "")
  if (args.size == 6) {
    brokers = args(0)
    cassandraHost = args(1)
    inputTopic = args(2)
    keySpace = args(3)
    tableRT = args(4)
    tableH = args(5)
  } else {
    brokers = "localhost:9092"
    cassandraHost = "127.0.0.1"
    inputTopic = "preds"
    keySpace = "demo"
    tableRT = "real_time_market_prices"
    tableH = "historical_prices"
    conf.setMaster("local[*]")
  }
  conf.set("spark.cassandra.connection.host", cassandraHost)
  val sc = new SparkContext(conf)

  val ssc = new StreamingContext(sc, Seconds(5))

  // Create direct kafka stream with brokers and topics
  val topicsSet = inputTopic.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
  val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
    ssc, kafkaParams, topicsSet)

  val tableData = Utils.parseMEnrichMessage(messages).map {
    case (msg, msgMap) =>
      val date = new java.util.Date()
      (msgMap.apply("prodId"), msgMap.apply("domain"), date, Utils.getPriceFromMsgMap(msgMap), msgMap.apply("title"))
  }
  tableData.saveToCassandra(keySpace, tableH)
  tableData.map(t => (t._1, t._2, t._4, t._5)).saveToCassandra(keySpace, tableRT)
  ssc.start()
  ssc.awaitTermination()

}