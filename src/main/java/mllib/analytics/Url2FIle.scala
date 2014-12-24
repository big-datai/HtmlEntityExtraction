package mllib.analytics
import scala.io.Source
import org.apache.spark.SparkContext
import java.io._
import java.net.URL
import sys.process._
import java.net.URL
import java.io.File
import scala.io.Source
import scala.util.Try

object Url2FIle {

  def url2Html(url: String) = {
    var html: String = ""
    try {
      html = Source.fromURL(url).toString
      val s = html.mkString
      val writer = new PrintWriter(new File("/Applications/workspace/mvnscala/" + url))
      writer.write(s)
      writer.close()
      //new URL(url) #> new File(url)
    } catch { case e: Exception => println("could not load url") }
  }
  def loadURL(file: String) = {

  }

  def parth(x: String) = {
    val y = x.split(",").filter(p => p.contains("http"))(0).toString()
    url2Html(y)
    println(y)
    y
  }
  def main(arg: Array[String]) {
    //val sc = new SparkContext("local[4]", "naivebayes")

    var urls: Array[String] = Array()
    Source.fromFile("/Applications/workspace/mvnscala/text.txt").getLines.foreach(x => urls :+= x.toString())

    urls.map(x => parth(x))
    //urls.map(x=>println(x))

    //url2Html("http://www.cabelas.com")

  }
  implicit class RichFile(val filename: String) extends AnyVal {

    def fileLines(encoding: String = "utf-8") = {
      Try(Source.fromFile(filename, encoding).getLines) toOption
    }

    def fileArray(encoding: String = "utf-8") = {
      fileLines(encoding) match {
        case Some(fl) => Some(fl.toArray)
        case None => None
      }
    }

    def fileString(encoding: String = "utf-8", delim: String = "\n") = {
      fileLines(encoding) match {
        case Some(fl) => Some(fl.mkString(delim))
        case None => None
      }
    }
  }
}
