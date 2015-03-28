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

object Transformer {

  def splitRawDataByURL(data: RDD[(String, Map[String, String])], trainingFraction: Double = 0.7): (RDD[(String, Map[String, String])], RDD[(String, Map[String, String])]) = {
    val testFraction = 1 - trainingFraction
    val dataByURL = data.map { r =>
      val url = r._2.apply("url")
      (url, r)
    }.groupBy(_._1)
    val splits = dataByURL.randomSplit(Array(trainingFraction, testFraction))
    val (training, test) = (splits(0).flatMap(l => l._2).map(_._2), splits(1).flatMap(l => l._2).map(_._2))
    (training, test)
  }

  def findTopKThreshold(values: Array[Double], k: Int): Double = {
    val _k = math.min(k, values.filter(v => v != 0.0).length) //number of tdidf features
    values.sorted.takeRight(_k)(0)
  }

  def getGreaterIndices(values: Array[Double], threshold: Double): Array[Int] = {
    (for (i <- values.indices if values(i) >= threshold) yield i).toArray
  }

  def projectByIndices(values: Array[Double], indices: Array[Int]): Array[Double] = {
    indices.map(i => values(i))
  }

  def getTopTFIDFIndices(k: Int, avgTFIDF: Array[Double]): Array[Int] = {
    val _threshold = findTopKThreshold(avgTFIDF, k)
    val _indices = getGreaterIndices(avgTFIDF, _threshold)
    _indices
  }

  def parseDataRow(row: (String, Map[String, String])): (Int, Seq[String], Double) = {
    val before = Utils.tokenazer(row._2.apply("text_before"))
    val after = Utils.tokenazer(row._2.apply("text_after"))
    val domain = Utils.getDomain(row._2.apply("url"))
    val location = Integer.valueOf(row._2.apply("location")).toDouble / (Integer.valueOf(row._2.apply("length")).toDouble)
    val parts = before ++ after
    val partsEmbedded = parts
    if (Utils.isTrueCandid(row._2, row._2))
      (1, partsEmbedded, location)
    else
      (0, partsEmbedded, location)

  }

  def gramsByN(data: String, number: Int): List[String] = {
    val chrData = data.toCharArray
    var i = 0
    var grams: List[String] = List()
    val lenght = chrData.length
    for (i <- 1 until lenght) {
      if (i + number < lenght) {
        val str = data.substring(i, i + number)
        grams = str :: grams
      }
    }
    grams
  }
  def gramsByNTokens(data: String, number: Int): List[String] = {
    val chrData = data.toCharArray
    var i = 0
    var grams: List[String] = List()
    val lenght = chrData.length
    for (i <- 1 until lenght) {
      if (i + number < lenght) {
        val str = data.substring(i, i + number)
        grams = str :: grams
      }
    }
    grams
  }
  def filterByPrice(row: (String, Map[String, String])): Boolean = {
    val before = row._2.apply("text_before")
    val after = row._2.apply("text_after")
    val price = row._2.apply("price")

    true
  }

  def gramsParser(row: (String, Map[String, String]), grams: Int): (Int, Seq[String], Double) = {
    val before = row._2.apply("text_before")
    val after = row._2.apply("text_after")
    val domain = Utils.getDomain(row._2.apply("url"))
    val data = before + after + domain
    val location = Integer.valueOf(row._2.apply("location")).toDouble / (Integer.valueOf(row._2.apply("length")).toDouble)
    val partsEmbedded = gramsByN(data, grams).toSeq
    if (Utils.isTrueCandid(row._2, row._2))
      (1, partsEmbedded, location)
    else
      (0, partsEmbedded, location)
  }
  def gramsParser(row: (String, Map[String, String]), grams: Int, grams2: Int): (Int, Seq[String], Double) = {
    val before = row._2.apply("text_before")
    val after = row._2.apply("text_after")
    val domain = Utils.getDomain(row._2.apply("url"))
    val data = before + after + domain
    val location = Integer.valueOf(row._2.apply("location")).toDouble / (Integer.valueOf(row._2.apply("length")).toDouble)
    val parts = gramsByN(data, grams).toSeq
    val parts2 = gramsByN(data, grams2).toSeq
    val partsEmbedded = parts ++ parts2
    if (Utils.isTrueCandid(row._2, row._2))
      (1, partsEmbedded, location)
    else
      (0, partsEmbedded, location)
  }
  /**
   * grams2 must be greater then 0
   */
  def gramsTFIDFParser(row: (String, Map[String, String]), grams: Int, grams2: Int): (Int, Seq[String], Double) = {
    val before = row._2.apply("text_before")
    val after = row._2.apply("text_after")
    val domain = Utils.getDomain(row._2.apply("url"))
    val data = before + after + domain
    val location = Integer.valueOf(row._2.apply("location")).toDouble / (Integer.valueOf(row._2.apply("length")).toDouble)
    val parts = gramsByN(data, grams).toSeq
    val parts2 = gramsByN(data, grams2).toSeq
    val partsEmbedded = parts ++ parts2 ++ Utils.tokenazer(data)
    if (Utils.isTrueCandid(row._2, row._2))
      (1, partsEmbedded, location)
    else
      (0, partsEmbedded, location)
  }
  
  def parseGramsTFIDFData(all: RDD[(String, Map[String, String])], grams: Int, grams2: Int): RDD[(Int, Seq[String], Double)] = {
    all.map(l => gramsTFIDFParser(l, grams, grams2))
  }
  
  def parseData(all: RDD[(String, Map[String, String])], grams: Int, grams2: Int): RDD[(Int, Seq[String], Double)] = {
    if (grams2 != 0)
      all.map(l => gramsParser(l, grams, grams2)).filter(l => l._2.length > 1)
    else
      all.map(l => gramsParser(l, grams)).filter(l => l._2.length > 1)
  }
  def parseData(all: RDD[(String, Map[String, String])]): RDD[(Int, Seq[String], Double)] = {
    all.map(parseDataRow).filter(l => l._2.length > 1)
  }
  def gramsParseData(all: RDD[(String, Map[String, String])]): RDD[(Int, Seq[String], Double)] = {
    all.map(parseDataRow).filter(l => l._2.length > 1)
  }
  def parseData4Test(raw: RDD[(String, Map[String, String])]): RDD[(String, (Int, String, String, Double, Seq[String], String))] = {
    raw.map { l =>
      val url = l._2.apply("url")
      val priceCandidate = l._2.apply("priceCandidate")
      val price = l._2.apply("price")
      val domain = Utils.getDomain(url)
      val (label, partsEmbedded, normalizedLocation) = parseDataRow(l)
      (url, (label, price, priceCandidate, normalizedLocation, partsEmbedded, domain))

    }.filter(l => l._2._5.length > 1)
  }

  def parseDataPerURL(raw: RDD[(String, Map[String, String])]): RDD[(String, (Int, Seq[String], Double, String))] = {
    raw.map { l =>
      val url = l._2.apply("url")
      val domain = Utils.getDomain(url)
      val (label, partsEmbedded, normalizedLocation) = parseDataRow(l)
      (url, (label, partsEmbedded, normalizedLocation, domain))

    }.filter(l => l._2._2.length > 1)
  }

  def parseDataRawPerURL(raw: RDD[(String, Map[String, String])]): RDD[(String, (Int, String, Double, String))] = {
    raw.map { l =>
      val url = l._2.apply("url")
      val domain = Utils.getDomain(url)
      val before = l._2.apply("text_before")
      val after = l._2.apply("text_after")
      val txt =  before+after
      val (label, partsEmbedded, normalizedLocation) = parseDataRow(l)
      (url, (label, txt, normalizedLocation, domain))

    }.filter(l => l._2._2.length > 1)
  }
  def tokenizeParsedDataByURLRow(row:(String, (Int, String, Double, String)),tokenize:Boolean,grams:Int,grams2:Int) ={
    val (url, (label, txt, normalizedLocation, domain)) = row  
    var newTokens :Seq[String]= Seq.empty
	if (tokenize)
	  newTokens = Utils.tokenazer(txt)
	if (grams > 0)
	  newTokens = newTokens++Transformer.gramsByN(txt, grams)
	if (grams2 > 0)
	  newTokens = newTokens++Transformer.gramsByN(txt, grams2)
	(url, (label, newTokens, normalizedLocation, domain))
	
  }
  def tokenizeParsedDataByURL(data:RDD[(String, (Int, String, Double, String))],tokenize:Boolean,grams:Int,grams2:Int)={
    data.map{tokenizeParsedDataByURLRow(_,tokenize,grams,grams2)}
  }

  
  def data2points(data: RDD[(Int, Seq[String], Double)], idf_vals: Array[Double], selected_ind_vals: Array[Int] = null, tf_model: HashingTF): RDD[LabeledPoint] = {
    data.map {
      case (lable, txt, location) =>
        val tf_vals_full = tf_model.transform(txt).toArray
        val  tf_vals = selected_ind_vals.map(i => tf_vals_full(i))
        val tfidf_vals = (tf_vals, idf_vals).zipped.map((d1, d2) => d1 * d2)
        val features = tfidf_vals ++ Array(location)
        val values = features.filter { l => l != 0 }
        val index = features.zipWithIndex.filter { l => l._1 != 0 }.map { l => l._2 }
        LabeledPoint(lable, Vectors.sparse(features.length, index, values))
    }
  }
def data2points(data: RDD[(Int, Seq[String], Double)], idf_vals: Array[Double], tf_model: HashingTF): RDD[LabeledPoint] = {
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

  
  
  def data2pointsPerURL(data: RDD[(String, (Int, Seq[String], Double, String))], idf_vals: Array[Double], selected_ind_vals: Array[Int], tf_model: HashingTF) = {
    data.map {
      case (url, (lable, txt, location, domain)) =>
        val tf_vals_full = tf_model.transform(txt).toArray
        val tf_vals = selected_ind_vals.map(i => tf_vals_full(i))
        val tfidf_vals = (tf_vals, idf_vals).zipped.map((d1, d2) => d1 * d2)
        val features = tfidf_vals ++ Array(location)
        val values = features.filter { l => l != 0 }
        val index = features.zipWithIndex.filter { l => l._1 != 0 }.map { l => l._2 }
        (url, LabeledPoint(lable, Vectors.sparse(features.length, index, values)))
    }
  }
  def filterData(data: RDD[LabeledPoint], unified_indx_idf: (Array[Int], Array[Double])): RDD[LabeledPoint] = {
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
  def dataSample(percent: Double, parsedData: RDD[(Int, Seq[String], Double)]): RDD[(Int, Seq[String], Double)] = {
    val splits = parsedData.randomSplit(Array(1 - percent, percent))
    splits(1)
  }

  def labelAndPredPerURL(model: GradientBoostedTreesModel, input_points: RDD[(String, LabeledPoint)]): RDD[(String, Double, Double)] = {
    val labelAndPreds = input_points.map {
      case (url, point) =>
        val prediction = model.predict(point.features)
        (url, point.label, prediction)
    }
    labelAndPreds
  }

  def buildTreeSubModels(model: GradientBoostedTreesModel,sizes:Array[Int]=Array()): IndexedSeq[GradientBoostedTreesModel] = {
    val algo = model.algo
    val trees = model.trees
    val treeW = model.treeWeights
    val numTrees = trees.length
    if (sizes.length==0)
    	for (i <- 1 to trees.size) yield new GradientBoostedTreesModel(algo, trees.take(i), treeW.take(i))
    else 
      for (i <- sizes) yield new GradientBoostedTreesModel(algo, trees.take(i), treeW.take(i))
  }

  def evaluateModel(labelAndPreds: RDD[(String, Double, Double)], model_i: GradientBoostedTreesModel) = {
    val tp = labelAndPreds.filter { case (url, l, p) => (l == 1) && (p == 1) }.count
    val tn = labelAndPreds.filter { case (url, l, p) => (l == 0) && (p == 0) }.count
    val fp = labelAndPreds.filter { case (url, l, p) => (l == 0) && (p == 1) }.count
    val fn = labelAndPreds.filter { case (url, l, p) => (l == 1) && (p == 0) }.count
    val sen = tp / (tp + fn).toDouble
    val spec = tn / (fp + tn).toDouble
    val prec = tp / (tp + fp).toDouble
    val predsByURL = labelAndPreds.groupBy(_._1).cache
    val urlCount = predsByURL.count
    val upperBound = predsByURL.filter { p => p._2.toList.filter(l => l._2 == 1 && l._3 == 1).size > 0 }.count.toDouble / urlCount.toDouble
    val lowerBound = predsByURL.filter { p => (p._2.size > 0 && p._2.toList.filter(l => l._2 == 1 && l._3 == 1).size > 0 && p._2.size == p._2.toList.filterNot(l => l._2 == 0 && l._3 == 1).size) }.count.toDouble / urlCount.toDouble
    predsByURL.unpersist()
    (model_i.trees.length, (tp, tn, fp, fn, sen, spec, prec, upperBound, lowerBound))
  }

  def labelAndPredPerURL(model: RandomForestModel, input_points: RDD[(String, LabeledPoint)]): RDD[(String, Double, Double)] = {
    val labelAndPreds = input_points.map {
      case (url, point) =>
        val prediction = model.predict(point.features)
        (url, point.label, prediction)
    }
    labelAndPreds
  }

  def labelAndPred(inputPoints: RDD[LabeledPoint], model: GradientBoostedTreesModel): RDD[(Double, Double)] = {
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
    val res = "sensitivity : " + tp / (tp + fn).toDouble + " specificity : " + tn / (fp + tn).toDouble + " precision : " + tp / (tp + fp).toDouble
    println(res)
    labelAndPreds
  }
  def labelAndPredRes(inputPoints: RDD[LabeledPoint], model: GradientBoostedTreesModel): String = {
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
    val res = "sensitivity : " + tp / (tp + fn).toDouble + " specificity : " + tn / (fp + tn).toDouble + " precision : " + tp / (tp + fp).toDouble
    res
  }
    def labelAndPredRes(inputPoints: RDD[LabeledPoint], model: SVMModel): String = {
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
    val res = "sensitivity : " + tp / (tp + fn).toDouble + " specificity : " + tn / (fp + tn).toDouble + " precision : " + tp / (tp + fp).toDouble
    res
  }
  def labelAndPred(inputPoints: RDD[LabeledPoint], model: RandomForestModel): RDD[(Double, Double)] = {
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