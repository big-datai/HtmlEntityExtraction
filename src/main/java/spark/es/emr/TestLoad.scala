
package spark.es.emr;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import breeze.macros.expand.args;

 object TestingLoad {
	
	def main(args : Array[String]) {
	    val logFile = "s3://pavlovP/*" 
	    val conf = new SparkConf().setAppName("es").set("master", "yarn-cluster")
	    
	    val sc = new SparkContext(conf)
	    val logData = sc.textFile(logFile)
	    println(logData.count+"       +++++++++++++++++++++++++              ")
	    val numAs = logData.filter(line => line.contains("a")).count()
	    val numBs = logData.filter(line => line.contains("b")).count()
	    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
	  }
}
