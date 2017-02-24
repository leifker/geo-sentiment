package org.apache.spark.ml.feature

import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.BinaryAttribute
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.{DoubleParam, ParamMap}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable.ArrayBuilder

/**
  * Created by dleifker on 2/23/17.
  */
class InverseBinarizer(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("inverseBinarizer"))

  /**
    * Param for threshold used to binarize continuous features.
    * The features greater than the threshold, will be binarized to 1.0.
    * The features equal to or less than the threshold, will be binarized to 0.0.
    * Default: 0.0
    * @group param
    */
  @Since("1.4.0")
  val threshold: DoubleParam =
  new DoubleParam(this, "threshold", "threshold used to binarize continuous features")

  /** @group getParam */
  @Since("1.4.0")
  def getThreshold: Double = $(threshold)

  /** @group setParam */
  @Since("1.4.0")
  def setThreshold(value: Double): this.type = set(threshold, value)

  setDefault(threshold -> 0.0)

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val schema = dataset.schema
    val inputType = schema($(inputCol)).dataType
    val td = $(threshold)

    val binarizerDouble = udf { in: Double => if (in > td) 0.0 else 1.0 }
    val binarizerVector = udf { (data: Vector) =>
      val indices = ArrayBuilder.make[Int]
      val values = ArrayBuilder.make[Double]

      data.foreachActive { (index, value) =>
        if (value > td) {
          indices += index
          values +=  1.0
        }
      }

      Vectors.sparse(data.size, indices.result(), values.result()).compressed
    }

    val metadata = outputSchema($(outputCol)).metadata

    inputType match {
      case DoubleType =>
        dataset.select(col("*"), binarizerDouble(col($(inputCol))).as($(outputCol), metadata))
      case _: VectorUDT =>
        dataset.select(col("*"), binarizerVector(col($(inputCol))).as($(outputCol), metadata))
    }
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    val outputColName = $(outputCol)

    val outCol: StructField = inputType match {
      case DoubleType =>
        BinaryAttribute.defaultAttr.withName(outputColName).toStructField()
      case _: VectorUDT =>
        StructField(outputColName, new VectorUDT)
      case _ =>
        throw new IllegalArgumentException(s"Data type $inputType is not supported.")
    }

    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ outCol)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): InverseBinarizer = defaultCopy(extra)
}

@Since("1.6.0")
object InverseBinarizer extends DefaultParamsReadable[InverseBinarizer] {

  @Since("1.6.0")
  override def load(path: String): InverseBinarizer = super.load(path)
}