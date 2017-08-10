package um.re.emr

import com.datastax.spark.connector._
import com.utils.messages.BigMessage
import org.apache.spark.{SparkConf, SparkContext}

object FillCassandraMessages extends App {
  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
  val sc = new SparkContext(conf)
  if (args.size == 5) {
    cassandraHost = args(0)
    keySpace = args(1)
    messagesCT = args(2)
    inputFilePath = args(3)
    numPartitions = args(4)
  } else {
    cassandraHost = "127.0.0.1"
    keySpace = "demo"
    messagesCT = "messages"
    inputFilePath = "/Users/dmitry/umbrella/seeds_sample"
    numPartitions = "200"
    conf.setMaster("local[*]")
  }
  conf.set("spark.cassandra.connection.host", cassandraHost)
  var (cassandraHost, keySpace, messagesCT, inputFilePath, numPartitions) = ("", "", "", "", "")
  try {


    val seeds = sc.objectFile[(String)](inputFilePath, numPartitions.toInt)
    println("Number of seeds read from file : " + seeds.count)
    val messagesRDD = seeds.map { line =>
      try {
        val msg = BigMessage.string2Message(line)
        (msg.getUrl(), msg.toJson().toString())
      } catch {
        case e: Exception => null
      }
    }.filter(_ != null)
    println("Number of messages sent to cassandra : " + messagesRDD.count)
    messagesRDD.saveToCassandra(keySpace, messagesCT)
  } catch {
    case e: Exception => {
      println("########  Somthing went wrong :( ")
      e.printStackTrace()
    }
  }

}