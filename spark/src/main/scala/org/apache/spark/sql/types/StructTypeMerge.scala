package org.apache.spark.sql.types

/**
  * Created by dleifker on 3/4/17.
  */
object StructTypeMerge {
  def merge(a: StructType, b: StructType): StructType = StructType.merge(a, b).asInstanceOf[StructType]
}
