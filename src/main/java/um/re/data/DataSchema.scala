package um.re.data

import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.{ HashingTF, IDF }
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.stat.Statistics
import um.re.transform.Transformer
import um.re.utils.{ UConf }
import org.apache.spark.mllib.regression.LabeledPoint
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

case class DataSchema(map: (String, Map[String, String]), location:Double, raw_text: String = null,  url: String = "",  domain: String = "", label:Boolean,
                      points: LabeledPoint = null, hash: HashingTF = null,pattern: String = null)