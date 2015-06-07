package um.re.emr

import java.io.File
import java.io.PrintWriter
import java.io.BufferedWriter
import java.io.FileWriter

object GenDMap extends App {
  val ROOT = "/Users/dmitry/umbrella/rawd/objects/Scores"

  def getListOfFiles(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isDirectory).toList.map(_.getName())
    } else {
      List[String]()
    }
  }

  val domainCodes = getListOfFiles(ROOT)
  val dMap = domainCodes.map { dCode =>
    val line = scala.io.Source.fromFile(ROOT+"/"+dCode + "/part-00000").getLines.take(1).toList.head
    val domain = line.substring(0,line.indexOf("("))
    domain + "\t" + dCode
  }.mkString("\n")

  val pw = new PrintWriter(new File("hello.txt"))
  pw.write("Hello, world")
  pw.close

  // FileWriter
  val file = new File(ROOT+"/dMap")
  val bw = new BufferedWriter(new FileWriter(file))
  bw.write(dMap)
  bw.close()

}