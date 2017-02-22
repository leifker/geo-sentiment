package com.github.leifker.spark.sentiment

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.util.Identifiable

/**
  * Created by dleifker on 2/21/17.
  */
class ReviewTokenizer(override val uid: String) extends Tokenizer {
  protected override def createTransformFunc: (String) => Seq[String] = NGramUtils.nGrams

  def this() = this(Identifiable.randomUID("tok"))
}
