package com.github.leifker.spark

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class Cacher(val uid: String) extends Transformer with DefaultParamsWritable {
  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF.cache()

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  def this() = this(Identifiable.randomUID("CacherTransformer"))
}
