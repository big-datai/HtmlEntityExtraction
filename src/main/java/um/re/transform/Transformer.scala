package um.re.transform

import scala.Array.canBuildFrom

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD

import um.re.utils.Utils

object Transformer {

  def parseData(all: RDD[(String, Map[String, String])]) = {
    all.map { l =>
      val before = Utils.tokenazer(l._2.apply("text_before"))
      val after = Utils.tokenazer(l._2.apply("text_after"))
      val domain = Utils.getDomain(l._2.apply("url"))
      val location = Integer.valueOf(l._2.apply("location")).toDouble / (Integer.valueOf(l._2.apply("length")).toDouble)
      val parts = before ++ after
      val parts_embedded = parts
      if ((l._2.apply("priceCandidate").contains(l._2.apply("price"))))
        (1, parts_embedded, location)
      else
        (0, parts_embedded, location)
    }.filter(l => l._2.length > 1)
  }
  def data2points(data: RDD[(Int, Seq[String], Double)], idf_vector: Array[Double], hashingTF: HashingTF) = {
    val idf_vals = idf_vector
    val tf_model = hashingTF
   data.map {
      case (lable, txt, location) =>
        val tf_vals = tf_model.transform(txt).toArray
        val tfidf_vals = (tf_vals, idf_vals).zipped.map((d1, d2) => d1 * d2)
        val features = tfidf_vals ++ Array(location)
        val values = features.filter { l => l != 0 }
        val index = features.zipWithIndex.filter { l => l._1 != 0 }.map { l => l._2 }
        LabeledPoint(lable, Vectors.sparse(features.length, index, values))
    }
  }
  def filterData(data: RDD[LabeledPoint], unified_indx_idf: (Array[Int], Array[Double])) = {
    val idf_vals = unified_indx_idf._2
    val unified_indx = unified_indx_idf._1
    data.map { point =>
      val label1 = point.label
      val tf_val = point.features.toArray
      val tf_vals_uniq = unified_indx.map(i => tf_val(i))
      val tfidf_vals = (tf_vals_uniq, idf_vals).zipped.map((d1, d2) => d1 * d2)
      val features = tfidf_vals
      val values = features.filter { l => l != 0 }
      val index = features.zipWithIndex.filter { l => l._1 != 0 }.map { l => l._2 }
      LabeledPoint(label1, Vectors.sparse(features.length, index, values))
    }
  }
  def dataSample(percent:Double,parsedData: RDD[(Int,Seq[String],Double)] )={   
  val splits = parsedData.randomSplit(Array(1-percent, percent))
  splits(1)
  }
  def labelAndPred(inputPoints: RDD[LabeledPoint], model: GradientBoostedTreesModel) = {
    val local_model = model
    val labelAndPreds = inputPoints.map { point =>
      val prediction = local_model.predict(point.features)
      (point.label, prediction)
    }
    val tp = labelAndPreds.filter { case (l, p) => (l == 1) && (p == 1) }.count
    val tn = labelAndPreds.filter { case (l, p) => (l == 0) && (p == 0) }.count
    val fp = labelAndPreds.filter { case (l, p) => (l == 0) && (p == 1) }.count
    val fn = labelAndPreds.filter { case (l, p) => (l == 1) && (p == 0) }.count
    println("tp : " + tp + ", tn : " + tn + ", fp : " + fp + ", fn : " + fn)
    println("sensitivity : " + tp / (tp + fn).toDouble + " specificity : " + tn / (fp + tn).toDouble + " precision : " + tp / (tp + fp).toDouble)
    labelAndPreds
  }
}