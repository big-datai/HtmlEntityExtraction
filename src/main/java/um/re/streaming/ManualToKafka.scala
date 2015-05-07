package um.re.streaming
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import kafka.producer._
import org.apache.spark.streaming.Seconds
import java.util.Properties
import um.re.utils.Utils
import com.utils.messages.MEnrichMessage 
import com.utils.messages.ModifiedMEnrichMessage
import kafka.serializer.DefaultEncoder

object ManualToKafka extends App{

 val sc = new SparkContext()
//  val ssc = new StreamingContext(sc, Seconds(5))
//val Array(brokers, inputTopic,outputTopic) = args
 val Array(brokers, topic) = Array("localhost:9092", "workers")
//Read Seeds From S3  
 val seeds = sc.parallelize(List(("mike.com", "http://www.mike.com","kaka1","k1","1","11","111","0","1","a","ccc","eee","1"), 
     ("dima.com", "http://www.dima.com","kaka2","k2","2","22","222","0","2","b","ccc","eee","1"), 
     ("eran.com", "http://www.eran.com","kaka3","k3","3","33","333","0","3","c","ccc","eee","1"), 
     ("deepricer.com", "http://www.deepricer.com","kaka4","k4","4","44","444","0","4","d","ccc","eee","1"))) 
 
       
       
    
//loop on seeds and apply ModifiedMEnrich on each line then apply toJsonModified .toString(). then .getBytes()

   val seeds2kafka = seeds.map{line=> 
     new ModifiedMEnrichMessage(line._1,line._2,line._3,line._4,line._5,line._6,line._7,line._8,line._9
         ,line._10,line._11,line._12,line._13).toJsonModified().toString().getBytes()
   
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



