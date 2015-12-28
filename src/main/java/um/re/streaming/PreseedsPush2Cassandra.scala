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
import com.utils.aws.AWSUtils

object PreseedsPush2Cassandra {
  def main(args: Array[String]) {
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
      cassandraHost = "localhost"
      inputTopic = "preseeds"
      keySpace = "demo"
      tableRT = "real_time_market_prices"
      tableH = "historical_prices"
      conf.setMaster("local[*]")
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

    var inputMessagesCounter = 0L
    var historicalFeedCounter = 0L
    var realTimeFeedCounter = 0L
    var exceptionCounter = 0L
    val ssc = new StreamingContext(sc, Seconds(2))
    //brokers = AWSUtils.getPrivateIp(brokers.substring(0, brokers.length() - 5)) + ":9092"
    try {
      // Create direct kafka stream with brokers and topics
      val topicsSet = inputTopic.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
      val inputMessages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        ssc, kafkaParams, topicsSet)
      //      inputMessages.count().foreachRDD(rdd => { inputMessagesCounter += rdd.first() })
      val historicalFeed = Utils.parseBigMessage(inputMessages).map {
        case (msg, msgMap) =>
          // val date = new java.util.Date()          //yyyy-mm-dd'T'HH:mm:ssZ  2015-07-15T16:25:52.325Z
          val date = DateTime.parse(msgMap.apply("lastUpdatedTime")).toDate() //,DateTimeFormat.forPattern("yyyy-mm-dd'T'HH:mm:ssZ"));
         //take total price including shipment
          val price=
            if(msgMap.apply("totalPrice").isEmpty())
              {msgMap.apply("price")} else{msgMap.apply("totalPrice")}
          
           
          //     val row = (msgMap.apply("ggId"), Utils.getDomain(msgMap.apply("url").substring(msgMap.apply("url").toLowerCase().indexOf("http"))), date,  
      //        msgMap.apply("price").replaceAll(",", ""), msgMap.apply("title"), msgMap.apply("url").substring(msgMap.apply("url").toLowerCase().indexOf("http")))
          val gglName=
            if(msgMap.apply("gglName").isEmpty())
              {Utils.getDomain(msgMap.apply("url"))} else{msgMap.apply("gglName")}
          val row = (msgMap.apply("ggId"),gglName,Utils.getDomain(msgMap.apply("url")), date,  
              price.replaceAll(",", ""), msgMap.apply("title"), msgMap.apply("url"))
    
          row
      }
     // val r = historicalFeed.filter(l=>l._2.contains("www.failed.com")||l._2.contains(""))
      historicalFeed.saveToCassandra(keySpace, tableH,SomeColumns("sys_prod_id", "store_id", "store_domain","tmsp", "price", "sys_prod_title", "url"))
     // historicalFeed.print(1)      //    historicalFeed.count().foreachRDD(rdd => { historicalFeedCounter += rdd.first() })
      //sys_prod_id | store_id | ggl_prod_id | hot_level | price | sys_prod_title | url
      val realTimeFeed = historicalFeed.map(t => (t._1, t._2, t._3,1 ,t._5, t._6, t._7))
      realTimeFeed.saveToCassandra(keySpace, tableRT, SomeColumns("sys_prod_id", "store_id", "store_domain" ,"hot_level", "price","sys_prod_title", "url"))
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
        println("#?#?#?#?#?#?#  ExceptionLocalizedMessage : " + e.getLocalizedMessage +
          "\n#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}