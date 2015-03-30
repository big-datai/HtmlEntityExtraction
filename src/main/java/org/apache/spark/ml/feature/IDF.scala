package org.apache.spark.ml.feature

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{ IntParam, ParamMap }
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{ VectorUDT, Vector }
import org.apache.spark.sql.types.DataType
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{ParamMap, IntParam, BooleanParam, Param}
import org.apache.spark.sql.types.{DataType, StringType, ArrayType}
/*
class IDF extends UnaryTransformer[Vector, Vector, IDF] {

  
  
override protected def createTransformFunc(paramMap: ParamMap): Vector => Vector = {
   
  null
  
  }


  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, false)

}

*/