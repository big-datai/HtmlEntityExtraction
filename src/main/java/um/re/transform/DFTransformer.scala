package um.re.transform

import scala.Array.canBuildFrom
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import um.re.utils.Utils
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.classification.SVMModel
import um.re.data._
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml._
import org.apache.spark.ml.param.ParamPair
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object DFTransformer {

  def rdd2DF(all: RDD[(String, Map[String, String])], sc: SparkContext): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val all_k: RDD[DataSchema] = all.map(parseRow)
    import sqlContext.implicits._
    //sqlContext.createDataFrame[DataSchema](all_k)
    all_k.toDF()
  }

  def parseRow(row: (String, Map[String, String])): DataSchema = {
    val raw_text = row._2.apply("text_before") + row._2.apply("text_after")
    val before = Utils.tokenazer(row._2.apply("text_before"))
    val after = Utils.tokenazer(row._2.apply("text_after"))
    val tokens_te = before ++ after
    val url = row._2.apply("url")
    val domain = Utils.getDomain(url)
    val location = Integer.valueOf(row._2.apply("location")).toDouble / (Integer.valueOf(row._2.apply("length")).toDouble)
    var flag = false
    if (Utils.isTrueCandid(row._2, row._2))
      flag = true
    new DataSchema(row, location, domain, url, domain, flag)
  }

}