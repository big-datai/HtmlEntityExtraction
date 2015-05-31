package um.re.streaming

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

object SeedsESLight2S3 extends App{

 // def main(str: Array[String]) {
    val conf=new SparkConf().setMaster("local")  
                            .setAppName("CountingSheep")
                            .set("spark.executor.memory", "1g")
   val sc = new SparkContext(conf)
    //  val ssc = new StreamingContext(sc, Seconds(5))
    //val Array(brokers, inputTopic,outputTopic) = args
//    val Array(brokers, topic) = Array("localhost:9092", "seeds")

    //Read Seeds From S3  
    //val path=Utils.S3STORAGE+"/dpavlov/seeds"
    val path = Utils.S3STORAGE + "/dpavlov/seeds20150516"
    val seeds = sc.objectFile[(String)](path, 200)
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val doms = sqlContext.jsonRDD(seeds)

    doms.registerTempTable("doms")

    val dist=sqlContext.sql("SELECT distinct(prodId) from doms")
    val freq=sqlContext.sql("SELECT COUNT(*) as counter ,prodId from doms GROUP BY prodId")
    freq.registerTempTable("freq")
    val res=sqlContext.sql ("SELECT * from freq WHERE counter>=20")
//select 10 prodId's with more than 20 repetitions
    val results=sqlContext.sql("SELECT * from doms where prodId IN ('3978576.0', '1.8522373E7', '1.5913822E7', '1.5912436E7', '1.1679757E7', '2.3802327E7', '1.8199135E7', '1.5911545E7', '2751031.0', '2.3801007E7')")
//save 2 S3
   results.toJSON.repartition(1).saveAsObjectFile(Utils.S3STORAGE+ "/dpavlov/ESlight20150516")
  
  
  }