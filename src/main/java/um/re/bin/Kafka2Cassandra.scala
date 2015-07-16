package um.re.streaming
/*
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import um.re.utils.Utils
import um.re.utils.{ UConf }
import um.re.utils.Utils
import com.datastax.spark.connector.streaming._
import kafka.serializer.StringDecoder
import org.apache.spark.{ SparkEnv, SparkConf, Logging }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
//import com.datastax.spark.connector.demo.streaming.embedded._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.SomeColumns
//import com.datastax.spark.connector.demo.Assertions
object Kafka2Cassandra extends App {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cleaner.ttl", "3600")
    .setMaster("local[*]")
    .setAppName("Streaming Kafka App")
  CassandraConnector(conf).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS streaming_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS streaming_test.key_value (key VARCHAR PRIMARY KEY, value INT)")
    session.execute(s"TRUNCATE streaming_test.key_value")
  }

  val sc = new SparkContext("local[4]", "test", conf)

  import com.datastax.spark.connector._

  val rdd = sc.cassandraTable("test", "kv")
  println(rdd.count)
  println(rdd.first)
  println(rdd.map(_.getInt("value")).sum)

  println("moshe")
  val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
  collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))

}
*/