package um.re.utils
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.JavaConversions._
import play.api.libs.json._
import java.util.regex.Pattern
import scala.util.control.Exception

object Utils {

  def extPatternLocationPair(pattern: String, html: String, new_length: Int) = {
    val size = html.length
    val res = Utils.hideSpecialChar(pattern).r.findAllMatchIn(html).
      map { m =>
        println(m.start + " the end: " + m.end)
        // println(" html sub string :"+html.substring(math.max(m.start - length, 0)))
        (m.start(1), html.substring(math.max(m.start(1) - new_length, 0), math.min(m.end(1) + new_length, size)))
      }.toMap
    res
  }

  def hideSpecialChar(price_pattern: String) = {
      val wildcard = "(.*?)"
	  val wild_index = price_pattern.indexOf(wildcard)
      val text_before = price_pattern.substring(0, wild_index)
      val text_after = price_pattern.substring(wild_index + wildcard.length)
      "(?:"+Pattern.quote(text_before)+")"+wildcard+"(?:"+Pattern.quote(text_after)+")"
    
    /*val price_match = price_pattern.replaceAll("[\t\n\r,]", "").replaceAll("[\\p{Blank}]{1,}", " ").replaceAll("\\(", "\\\\(")
      .replaceAll("\\)", "\\\\)").replaceAll("\\[", "\\\\[").replaceAll("\\]", "\\\\]").replaceAll("\\$", "\\\\\\$")
      .replaceAll("\\.", "\\\\.").replaceAll("\\*", "\\\\*").replaceAll("\\?", "\\\\?").replaceAll("\\+", "\\\\+")
      .replace("\\(\\.\\*\\?\\)", "(.*?)");
    price_match*/
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