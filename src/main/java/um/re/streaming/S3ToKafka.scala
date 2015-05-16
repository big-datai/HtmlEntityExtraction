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

object S3ToKafka { //}extends App {

  def main(str: Array[String]) {
    val conf=new SparkConf().setMaster("local")  
                            .setAppName("CountingSheep")
                            .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    //  val ssc = new StreamingContext(sc, Seconds(5))
    //val Array(brokers, inputTopic,outputTopic) = args
    val Array(brokers, topic) = Array("localhost:9092", "seeds")

    //Read Seeds From S3  
    //val path=Utils.S3STORAGE+"/dpavlov/seeds"
    val path = "/Users/dmitry/umbrella/seeds_sample"
    val seeds = sc.objectFile[(String)](path, 200)

    //loop on seeds and apply ModifiedMEnrich on each line then apply toJsonModified .toString(). then .getBytes()

   // MEnrichMessage(String domain, String url, String title,  String prod_id, String price_patterns, String price, String price_prop1, String price_prop_anal,
    //  String shipping, String last_scraped_time, String last_updated_time, String price_updated) 
   /* 
    val seeds2kafka = seeds.map { line =>
      new MEnrichMessage(line._1, line._2, line._3, line._4, line._5, line._6, line._7, line._8,
        line._9, line._10, line._11, line._12).toJson().toString().getBytes()
    }
    //Producer: launch the Array[Byte]result into kafka      
    seeds2kafka.foreachPartition { p =>
      val props = new Properties()
      props.put("metadata.broker.list", brokers)
      props.put("serializer.class", "kafka.serializer.DefaultEncoder")

      @transient val config = new ProducerConfig(props)
      @transient val producer = new Producer[String, Array[Byte]](config)
      p.foreach(rec => producer.send(new KeyedMessage[String, Array[Byte]](topic, rec)))
      producer.close()
    }

    // ssc.start()
    // ssc.awaitTermination()
     * 
     */
  }
}


