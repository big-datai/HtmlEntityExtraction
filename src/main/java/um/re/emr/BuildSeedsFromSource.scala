package um.re.emr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import um.re.utils.UConf
import um.re.utils.Utils
import org.apache.spark.storage.StorageLevel

/**
 * @author mike
 */
object BuildSeedsFromSource {
  def main(args:Array[String]){
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    var (sourcePath, seedsDest, numPartitions,dMapPath,filterFlag) = ("", "", "", "", "")
    if (args.size == 5) {
      sourcePath = args(0)
      seedsDest = args(1)
      numPartitions = args(2)
      dMapPath = args(3)
      filterFlag = args(4).toLowerCase()
    } else {
      //by default all in root folder of hdfs
      sourcePath = Utils.S3STORAGE + "/dpavlov/es/source20150516"
      seedsDest = Utils.S3STORAGE+ Utils.SEEDS2S3
      numPartitions = "100"
      dMapPath = Utils.S3STORAGE+"/dpavlov/models/dMapNew"
      filterFlag = "true"
      conf.setMaster("yarn-client")
    }
    val sc = new SparkContext(conf)
    try{
      var relevantDomainsBC = sc.broadcast(Array(""))
      if (filterFlag.toBoolean)
        relevantDomainsBC = sc.broadcast(sc.textFile((dMapPath), 1).collect().mkString("\n").split("\n").map(l => l.split("\t")(0)))
      val uConf = new UConf(sc, numPartitions.toInt)
      val sourceData = uConf.getDataFromS3(sourcePath).persist(StorageLevel.MEMORY_AND_DISK)
      println("!@!@!@!@!   sourceData count : "+sourceData.count())
      val filteredSourceData = sourceData 
      .filter(l=> !filterFlag.toBoolean||(relevantDomainsBC.value.contains(l._2.apply("domain"))) )
      .map(l => Utils.map2JsonString(l._2)).persist(StorageLevel.MEMORY_AND_DISK)
      sourceData.unpersist(false)
      println("!@!@!@!@!   filteredSourceData count : "+filteredSourceData.count())
      filteredSourceData.repartition(numPartitions.toInt).saveAsObjectFile(Utils.S3STORAGE+ Utils.SEEDS2S3)
      filteredSourceData.unpersist(false)
    } catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        e.printStackTrace()
      }
    }

  }
}