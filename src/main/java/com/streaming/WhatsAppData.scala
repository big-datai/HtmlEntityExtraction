
package com.streaming

import scala.io.Source
import java.io._

case class Data(date: String, name: String, msg: String)

object Statistics extends App {

  val fw = new FileWriter("test.txt");
  val positive = List("love")//, "hugs", "kiss","xo","xoxo","hug")
  println("sdfas") 

  val lines = Source.fromFile("//Users/dmitry//Michelle.txt").getLines
  //> lines  : Iterator[String] = non-empty iterator
  var list: List[Data] = Nil
  for (line <- lines) {
    val i_date = line.indexOf(",")
    val date = line take (i_date)
    var name = line drop (line.indexOf(" AM: ") + 4)
    if (-1 == line.indexOf(" AM: ")) {
      name = line drop (line.indexOf(" PM: ") + 4)
    }
    var fullname = name take (name.indexOf(":"))
    val message = name drop (name.indexOf(":") + 2)
    //println(date.split("/").take(1) mkString)

    list = Data(date.split("/").take(1) mkString, fullname, message) :: list
    
    if(fullname.contains("dima")){
        fullname="Dmitry"
      }
     if(fullname.contains("Kosich")){
        fullname="Michelle"
      }
    //printToFile(date+" , " + fullname + " , " + message+"\n")
    if (date.split("/").take(1).mkString != "")
      fw.write(date.split("/").drop(1).take(1).mkString + " | " + date.split("/").take(1).mkString + " | " + fullname + " | " + message.split(" ").toList.count(positive contains _) + "\n");
    println(message.split(" ").toList)

  }
  fw.close()
  //var some = list.groupBy(_.date).map(x => (x, list.count(_ == x)))

}