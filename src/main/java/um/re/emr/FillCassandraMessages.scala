package um.re.emr

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.utils.messages.MEnrichMessage
import com.datastax.spark.connector._

object FillCassandraMessages extends App {
  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)

  var (cassandraHost, keySpace, messagesCT,inputFilePath,numPartitions ) = ("", "", "", "", "")
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

  val sc = new SparkContext(conf)
  try{
    
  
  val seeds = sc.objectFile[(String)](inputFilePath, numPartitions.toInt)
  println("Number of seeds read from file : "+seeds.count )
  val messagesRDD = seeds.map { line =>
    try{
      val msg = MEnrichMessage.string2Message(line)
      (msg.getUrl() , msg.toJson().toString())
    } catch {
      case e: Exception => null
    }
  }.filter(_ != null)
  println("Number of messages sent to cassandra : "+messagesRDD.count )
  messagesRDD.saveToCassandra(keySpace, messagesCT)
  } catch {
    case e: Exception => {
      println("########  Somthing went wrong :( ")
      e.printStackTrace()
    }
  }
  
}