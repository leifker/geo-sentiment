package com.github.leifker.spark.sentiment

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

/**
  * Created by dleifker on 2/21/17.
  */
class ReviewTokenizer(override val uid: String) extends UnaryTransformer[String, Seq[String], ReviewTokenizer] with DefaultParamsWritable {

  /**
    * Number of features.  Should be > 0.
    * (default = 3)
    * @group param
    */
  val maxNGram = new IntParam(this, "maxNGrams", "max ngram (> 0)", ParamValidators.gt(0))

  /** @group getParam */
  def getMaxNGram: Int = $(maxNGram)

  /** @group setParam */
  def setMaxNGram(value: Int): this.type = set(maxNGram, value)

  setDefault(maxNGram -> 3)

  protected override def createTransformFunc: (String) => Seq[String] = NGramUtils.nGrams(_, getMaxNGram)

  def this() = this(Identifiable.randomUID("reviewtok"))

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  override def copy(extra: ParamMap): ReviewTokenizer = defaultCopy(extra)
}

object ReviewTokenizer extends DefaultParamsReadable[ReviewTokenizer] {
  override def load(path: String): ReviewTokenizer = super.load(path)
}