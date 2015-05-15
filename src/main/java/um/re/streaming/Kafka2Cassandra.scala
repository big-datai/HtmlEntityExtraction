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
import org.apache.spark.{SparkEnv, SparkConf, Logging}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import com.datastax.spark.connector.demo.streaming.embedded._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.demo.Assertions
object Kafka2Cassandra extends App {

val sc = new SparkConf(true)
    .set("spark.cassandra.connection.host", SparkMaster)
    .set("spark.cleaner.ttl", "3600")
    .setMaster("local[12]")
    .setAppName("Streaming Kafka App")
    
     CassandraConnector(sc).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS streaming_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS streaming_test.key_value (key VARCHAR PRIMARY KEY, value INT)")
    session.execute(s"TRUNCATE streaming_test.key_value")
  }

}
*/