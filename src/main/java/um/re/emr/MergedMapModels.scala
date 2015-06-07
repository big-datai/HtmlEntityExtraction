package um.re.emr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MergedMapModels extends App {

  val conf = new SparkConf().setMaster("local[*]")
    .setAppName("CountingSheep")
    .set("spark.executor.memory", "5g")
  val sc = new SparkContext(conf)

  val dmap = sc.textFile("dMap").map{l=>   
    val par=l.split(", ")
    if(par.length==2)
      (par.apply(1),par.apply(0))
     else
       null
  
  }.filter{l=>l!=null}

  println("hello  "+dmap.count+dmap.take(1).mkString(" "))
 
 
  val codes = sc.textFile("domainCodes.txt").map{l=>(l,l)}
  
  println(codes.take(1))
  codes.count
  val res=dmap.join(codes).map{l=>l._2._1}.distinct.coalesce(1)
  
  res.foreach(println)
  res.saveAsTextFile("res")

}