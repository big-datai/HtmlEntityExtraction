package um.re.data

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint

case class DataSchema(map: (String, Map[String, String]), location: Double, raw_text: String = null, url: String = "", domain: String = "",
                      label: Boolean = false, tokens: Seq[String] = null, hash: Vector = null, grams4: Seq[String] = null,
                      grams5: Seq[String] = null, points: LabeledPoint = null, score: Scores = null, pattern: String = null)

case class Scores(res: Map[String, IndexedSeq[(Int, (Long, Long, Long, Long, Double, Double, Double, Double, Double))]])

case class Url(url: String)