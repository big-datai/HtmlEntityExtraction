package um.re.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import java.util.Properties
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import um.re.utils.Utils
import com.utils.messages.MEnrichMessage

object FillSeedsByProdFreq extends App {
  val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)

  var (timeUnitSec, brokers, outputTopic, cassandraHost, keySpace, prodUpdateRateCT, matchesBySysProdIdCT, messagesCT , pullRatesStep ) = ("", "", "", "", "", "", "", "", "")
  if (args.size == 9) {
    timeUnitSec = args(0)
    brokers = args(1)
    outputTopic = args(2)
    cassandraHost = args(3)
    keySpace = args(4)
    prodUpdateRateCT = args(5)
    matchesBySysProdIdCT = args(6)
    messagesCT = args(7)
    pullRatesStep = args(8)
  } else {
    timeUnitSec = "60"
    brokers = "localhost:9092"
    outputTopic = "seeds"
    cassandraHost = "127.0.0.1"
    keySpace = "demo"
    prodUpdateRateCT = "prod_update_rate"
    matchesBySysProdIdCT = "matching_prods_by_sys_prod_id"
    messagesCT = "messages"
    pullRatesStep = "60"
    conf.setMaster("local[*]")
  }
  conf.set("spark.cassandra.connection.host", cassandraHost)

  var exceptionCounter = 0L
  var step = 0L
  var resetStep = 0L
  
  val sc = new SparkContext(conf)
  var rates: Set[Int] = Set.empty
  //TODO collect counts and output them
  val ssc = new StreamingContext(sc, Seconds(timeUnitSec.toInt))
  try {
    if ((step == resetStep)||(step==Long.MaxValue)) { //reset step and set the next time to reset
      rates = ssc.cassandraTable(keySpace, prodUpdateRateCT).select("update_window").distinct.collect.map(r => r.getInt("update_window")).toSet
      resetStep = Utils.lcm(rates)
      step = 0L
    }
    step += 1
    if (step == pullRatesStep.toLong){//update rates each "pullRatesStep" iterations 
      rates = ssc.cassandraTable(keySpace, prodUpdateRateCT).select("update_window").distinct.collect.map(r => r.getInt("update_window")).toSet
      val newRatesLCM = Utils.lcm(rates)
      resetStep = newRatesLCM
      if(newRatesLCM < step){
        resetStep = (step/newRatesLCM)*newRatesLCM+newRatesLCM
      }
    }
    val rates2Update = rates.filter(rate => rate % step == 0)
    if (!rates2Update.isEmpty) { // skip if there is no product to update at this iteration
      val sysProdList = ssc.cassandraTable(keySpace, prodUpdateRateCT).select("sys_prod_id").where("update_window = ?", rates2Update)
        .map(r => r.getString("sys_prod_id"))
      val urlList = ssc.cassandraTable(keySpace, matchesBySysProdIdCT).select("url").where("sys_prod_id = ? ", sysProdList)
        .map(r => r.getString("url"))
      val messages = ssc.cassandraTable(keySpace, messagesCT).select("json_message").where("url = ?", urlList)
        .map(r => MEnrichMessage.string2Message(r.getString("json_message")).toJson().toString().getBytes()) 
        
      Utils.pushByteRDD2Kafka(messages, outputTopic, brokers)  
      
    }

  } catch {
    case e: Exception => {
      exceptionCounter += 1
    }
  }

}