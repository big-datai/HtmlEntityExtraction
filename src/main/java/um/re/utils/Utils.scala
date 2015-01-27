package um.re.utils
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.JavaConversions._
import play.api.libs.json._

object Utils {

  def extPatternLocationPair(pattern: String, html: String, length: Int) = {
    val size = html.length
    val res = Utils.hideSpecialChar(pattern).r.findAllMatchIn(html).
      map { m =>
        println(m.start + " the end: " + m.end)
        // println(" html sub string :"+html.substring(math.max(m.start - length, 0)))
        (m.start, html.substring(math.max(m.start - length, 0), math.min(m.end + length, size)))
      }.toMap
    res
  }

  def hideSpecialChar(price_pattern: String) = {
    val price_match = price_pattern.replaceAll("[\t\n\r,]", "").replaceAll("[\\p{Blank}]{1,}", " ").replaceAll("\\(", "\\\\(")
      .replaceAll("\\)", "\\\\)").replaceAll("\\[", "\\\\[").replaceAll("\\]", "\\\\]").replaceAll("\\$", "\\\\\\$")
      .replaceAll("\\.", "\\\\.").replaceAll("\\*", "\\\\*").replaceAll("\\?", "\\\\?").replaceAll("\\+", "\\\\+")
      .replace("\\(\\.\\*\\?\\)", "(.*?)");
    price_match
  }
  
  /**
   * this function removes more then 3 spaces from string
   */
  def threePlusTrim(str: String): String = {
    null
  }

  /**
   * this function removes all spaces around string
   */
  def trimFromSides(str: String): String = {
    null
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
  // helper function to convert Map to a Writable
  //http://loads.pickle.me.uk/2013/11/12/spark-and-elasticsearch.html
  def toWritable(map: Map[String, String]) = {
    val m = new MapWritable
    for ((k, v) <- map)
      m.put(new Text(k), new Text(v))
    m
  }

  def mapWritableToInput(in: MapWritable): Map[String, String] = {
    in.map { case (k, v) => (k.toString, v.toString) }.toMap
  }
}