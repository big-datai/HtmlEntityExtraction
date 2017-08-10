package um.re.utils

import java.io.{File, _}

import scala.Array.canBuildFrom
import scala.io.Source
import scala.util.Try

object Url2FIle {

  def loadURL(file: String) = {

  }

  def main(arg: Array[String]) {
    //val sc = new SparkContext("local[4]", "naivebayes")

    var urls: Array[String] = Array()
    Source.fromFile("/Applications/workspace/mvnscala/text.txt").getLines.foreach(x => urls :+= x.toString())

    urls.map(x => parth(x))
    //urls.map(x=>println(x))

    //url2Html("http://www.cabelas.com")

  }

  def parth(x: String) = {
    val y = x.split(",").filter(p => p.contains("http"))(0).toString()
    url2Html(y)
    println(y)
    y
  }

  def url2Html(url: String) = {
    var html: String = ""
    try {
      html = Source.fromURL(url).toString
      val s = html.mkString
      val writer = new PrintWriter(new File("/Applications/workspace/mvnscala/" + url))
      writer.write(s)
      writer.close()
      //new URL(url) #> new File(url)
    } catch {
      case e: Exception => println("could not load url")
    }
  }

  implicit class RichFile(val filename: String) extends AnyVal {

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

    def fileLines(encoding: String = "utf-8") = {
      Try(Source.fromFile(filename, encoding).getLines) toOption
    }
  }

}
