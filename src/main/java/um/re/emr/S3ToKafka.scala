package um.re.emr

import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import kafka.producer._
import org.apache.spark.streaming.Seconds
import java.util.Properties
import um.re.utils.Utils
import com.utils.messages.MEnrichMessage
import kafka.serializer.DefaultEncoder
import com.utils.aws.AWSUtils

object S3ToKafka { //}extends App {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    var (brokers, outputTopic, inputPath, numPartitions) = ("", "", "", "")
    if (args.size == 4) {
      brokers = args(0)
      outputTopic = args(1)
      inputPath = args(2)
      numPartitions = args(3)
    } else {
      brokers = "localhost:9092"
      outputTopic = "seeds"
      inputPath = "/Users/mike/umbrella/seeds_sample"
      numPartitions = "200"
      conf.setMaster("local[*]")
    }
    // try getting inner IPs
    try{
      val brokerIP = brokers.split(":")(0) 
      val brokerPort = brokers.split(":")(1) 
      val innerBroker = AWSUtils.getPrivateIp(brokerIP) + ":"+brokerPort
      brokers = innerBroker
    } catch {
      case e: Exception => {
        println("#?#?#?#?#?#?#  Couldn't get inner broker IP, using : "+brokers +
              "\n#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
              "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
    
    val sc = new SparkContext(conf)
    try {
      
      val rawSeeds = sc.objectFile[(String)](inputPath, numPartitions.toInt).cache
      val parsedSeeds = rawSeeds.map { line =>
        try { MEnrichMessage.string2Message(line).toJson().toString().getBytes() }
        catch {
          case e: Exception => null
        }
      }.filter { _!=null }
      //Producer: launch the Array[Byte]result into kafka      
      Utils.pushByteRDD2Kafka(parsedSeeds, outputTopic, brokers)
      println("!@!@!@!@!   rawSeeds Count:"+rawSeeds.count())
      println("!@!@!@!@!   parsedSeeds Count:"+parsedSeeds.count())
    } catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionLocalizedMessage : "+ e.getLocalizedMessage+
            "\n#?#?#?#?#?#?#  ExceptionMessage : "+e.getMessage+
            "\n#?#?#?#?#?#?#  ExceptionStackTrace : "+e.getStackTraceString)
      }
    }
  }
}


