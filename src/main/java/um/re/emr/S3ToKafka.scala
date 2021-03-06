package um.re.emr

import com.utils.aws.AWSUtils
import org.apache.spark.{SparkContext, _}
import um.re.utils.Utils

object S3ToKafka { //}extends App {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
    //./spark-shell --jars /home/ec2-user/core-1.0-SNAPSHOT-jar-with-dependencies.jar
    var (brokers, outputTopic, inputPath, numPartitions) = ("localhost:9092", "seeds1", "s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seedsReduce1_MonAug24154530UTC2015", "600")
    //var (brokers, outputTopic, inputPath, numPartitions) = ("localhost:9092", "preseeds", "s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seedsReduce1_MonAug24154530UTC2015", "60")
    if (args.size == 4) {
      brokers = args(0)
      outputTopic = args(1)
      inputPath = args(2)
      numPartitions = args(3)
    } else {
      brokers = "54.225.122.3:9092"
      outputTopic = "seeds"
      inputPath = "s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seedsFilteredMonAug24142239UTC2015"
      numPartitions = "3000"
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

      brokers = AWSUtils.getPrivateIp(brokers.substring(0, brokers.length() - 5)) + ":9092"

      val rawSeeds = sc.textFile(inputPath, numPartitions.toInt).sample(true, 0.001, 123) //sc.objectFile[(String)](inputPath, numPartitions.toInt).cache
      val parsedSeeds = rawSeeds.map { line =>
        try {
          line.getBytes
        }
        catch {
          case e: Exception => null
        }
      }.filter {
        _ != null
      }
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


