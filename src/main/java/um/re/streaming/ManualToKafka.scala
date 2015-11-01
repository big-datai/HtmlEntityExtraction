package um.re.streaming
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import kafka.producer._
import org.apache.spark.streaming.Seconds
import java.util.Properties
import um.re.utils.Utils
import com.utils.messages.BigMessage 
import kafka.serializer.DefaultEncoder
import org.apache.spark.SparkConf

object ManualToKafka { //extends App{
def main (str:Array[String]) {
val conf = new SparkConf().setMaster("local[1]").setAppName("Test")
 val sc = new SparkContext(conf)
//  val ssc = new StreamingContext(sc, Seconds(5))
//val Array(brokers, inputTopic,outputTopic) = args
 val Array(brokers, topic) = Array("localhost:9092", "workers")
//Read Seeds From S3  
 val json="""{
    "url": "http://luddensnatural.com/Herbal-Deodorants-Summer-Spice-Roll-Ons-3oz-p23715.html?utm_source=google-shopping&utm_medium=organic",
    "title": "Nature's Gate Deodorant, Summer Spice, Roll-on - 3 oz",
    "patternsHtml": "fsadfs!--pricedynamicallydisplayedhere-->$(.*?)</div></div><!--##PRICE_END##-->|||",
    "price": "4.20",
    "html": "no htmls",
    "patternsText": "dorantsSummerSpiceRoll-Ons3oz.BacktoList$(.*?)SKU: 201842UPC: 078347555057Qty: 1Addto|||",
    "shipping": "0",
    "prodId": "24358971",
    "domain": "no",
    "lastScrapedTime": "2014-11-17T06: 43: 13.921Z",
    "lastUpdatedTime": "2014-11-30T21: 14: 47.529Z",
    "updatedPrice": "4.2"
}"""
val seeds=sc.parallelize(List(json
))

//loop on seeds and apply ModifiedMEnrich on each line then apply toJsonModified .toString(). then .getBytes()
    val seeds2kafka = seeds.map { line =>
      BigMessage.string2Message(line).toJson().toString().getBytes()
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
}
}


