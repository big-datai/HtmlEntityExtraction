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
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object Preds2CassandraDev {
  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .setAppName(getClass.getSimpleName)

    var (timeInterval,brokers, cassandraHost, inputTopic, keySpace, tableRT, tableH) = ("", "", "", "", "", "", "")
    if (args.size == 7) {
      timeInterval = args(0) 
      brokers = args(1)
      cassandraHost = args(2)
      inputTopic = args(3)
      keySpace = args(4)
      tableRT = args(5)
      tableH = args(6)
    } else {
      timeInterval = "2"
      brokers = "localhost:9092"
      cassandraHost = "127.0.0.1"
      inputTopic = "preds"
      keySpace = "demo"
      tableRT = "real_time_market_prices"
      tableH = "historical_prices"
      conf.setMaster("local[*]")
    }
    conf.set("spark.cassandra.connection.host", cassandraHost)
    var inputMessagesCounter = 0L
    var historicalFeedCounter = 0L
    var realTimeFeedCounter = 0L
    var exceptionCounter = 0L
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(timeInterval.toInt))
    try {
      // Create direct kafka stream with brokers and topics
      val topicsSet = inputTopic.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
      val inputMessages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc, kafkaParams, topicsSet)

//      inputMessages.count().foreachRDD(rdd => { inputMessagesCounter += rdd.first() })

      val historicalFeed = Utils.parseMEnrichMessage(inputMessages).map {
        case (msg, msgMap) =>
         // val date = new java.util.Date()
          
          //yyyy-mm-dd'T'HH:mm:ssZ
          val date = DateTime.parse(msgMap.apply("lastUpdatedTime"),DateTimeFormat.forPattern("yyyy-mm-dd'T'HH:mm:ssZ"));
          (msgMap.apply("prodId"), msgMap.apply("domain"), date, Utils.getPriceFromMsgMap(msgMap), msgMap.apply("title"))
      }
      historicalFeed.saveToCassandra(keySpace, tableH)
//    historicalFeed.count().foreachRDD(rdd => { historicalFeedCounter += rdd.first() })

      val realTimeFeed = historicalFeed.map(t => (t._1, t._2, t._4, t._5))
      realTimeFeed.saveToCassandra(keySpace, tableRT)
/*      realTimeFeed.count().foreachRDD { rdd =>
        { realTimeFeedCounter += rdd.first() }
        println("!@!@!@!@!   inputMessagesCounter " + inputMessagesCounter)
        println("!@!@!@!@!   historicalFeedCounter " + historicalFeedCounter)
        println("!@!@!@!@!   realTimeFeedCounter " + realTimeFeedCounter)
        println("!@!@!@!@!   exceptionCounter " + exceptionCounter)
      }
*/
    } catch {
      case e: Exception => {
        exceptionCounter += 1
        println("oops somthing went wrong :(")
        println("#?#?#?#?#?#?#  ExceptionLocalizedMessage : "+ e.getLocalizedMessage+
            "\n#?#?#?#?#?#?#  ExceptionMessage : "+e.getMessage+
            "\n#?#?#?#?#?#?#  ExceptionStackTrace : "+e.getStackTraceString)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}