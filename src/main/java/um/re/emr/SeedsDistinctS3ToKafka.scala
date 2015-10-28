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
import um.re.utils.{ UConf }
import com.utils.messages.MEnrichMessage
import kafka.serializer.DefaultEncoder
import com.utils.aws.AWSUtils

object SeedsDistinctS3ToKafka { //}extends App {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
//./spark-shell --jars /home/ec2-user/core-1.0-SNAPSHOT-jar-with-dependencies.jar
    var (brokers, outputTopic, inputPath, numPartitions) = ("localhost:9092", "distinctTitlesSeeds", "s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seedsFilteredMonAug24142239UTC2015", "600")
    if (args.size == 4) {
      brokers = args(0)
      outputTopic = args(1)
      inputPath = args(2)
      numPartitions = args(3)
    } else {
      var brokers = "localhost:9092"
      var outputTopic = "distinctTitlesSeeds"
      var inputPath = "s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seeds170820151439825456871"
      var numPartitions = "3000"
      //conf.setMaster("local[*]")

    }
    // try getting inner IPs
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

    val sc = new SparkContext(conf)

    try {

     brokers= AWSUtils.getPrivateIp(brokers.substring(0, brokers.length() - 5)) + ":9092"
    //   val dataSeeds = new UConf(sc, 1) 
    //   val raw = dataSeeds.getDataFromS3("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seeds20150516")
      val rawSeeds = sc.textFile(inputPath, numPartitions.toInt) //sc.objectFile[(String)](inputPath, numPartitions.toInt).cache
      val fetchTitle2rawSeeds= rawSeeds.map { line =>
        try {(MEnrichMessage.string2Message(line).getTitle(), MEnrichMessage.string2Message(line)) }
        catch {
          case e: Exception => null
        }
      }.filter { _ != null }
      val ditinctTitle2rawSeeds=fetchTitle2rawSeeds.reduceByKey((x,y)=>x).map(l=>l._2)   
      val parsedSeeds = ditinctTitle2rawSeeds.map { line =>
        try { line.toJson().toString().getBytes() }
        catch {
          case e: Exception => null
        }
      }.filter { _ != null }
      //Producer: launch the Array[Byte]result into kafka      
      Utils.pushByteRDD2Kafka(parsedSeeds, outputTopic, brokers)
      println("!@!@!@!@!   rawSeeds Count:" + rawSeeds.count())
      println("!@!@!@!@!   parsedSeeds Count:" + parsedSeeds.count())
    } catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionLocalizedMessage : " + e.getLocalizedMessage +
          "\n#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
  }
}


