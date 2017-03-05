package org.apache.spark.ml

import com.github.fommil.netlib.F2jBLAS
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.tuning.{CrossValidatorModel, CrossValidatorParams}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{StructType, StructTypeMerge}

/**
  * K-fold cross validation.
  */
class MultiCrossValidator (override val uid: String)
  extends Estimator[CrossValidatorModel]
    with CrossValidatorParams with Logging {

  def this() = this(Identifiable.randomUID("multicv"))

  private val f2jBLAS = new F2jBLAS

  /**
    * param for the estimator to be validated
    *
    * @group param
    */
  val estimators: Param[Array[Estimator[_]]] = new Param(this, "estimators", "estimators for selection")

  /** @group getParam */
  def getEstimators: Array[Estimator[_]] = $(estimators)

  /**
    * param for estimator param maps
    *
    * @group param
    */
  val estimatorsParamMaps: Param[Array[Array[ParamMap]]] =
    new Param(this, "estimatorsParamMaps", "param maps for the estimators")

  /** @group getParam */
  def getEstimatorsParamMaps: Array[Array[ParamMap]] = $(estimatorsParamMaps)

  /** @group setParam */
  def setEstimators(value: Array[Estimator[_]]): this.type = {
    set(estimators, value)
    set(estimator, value.head)
  }

  /** @group setParam */
  def setEstimatorsParamMaps(value: Array[Array[ParamMap]]): this.type = {
    set(estimatorsParamMaps, value)
    set(estimatorParamMaps, value.head)
  }

  /** @group setParam */
  def setEstimator(value: Estimator[_]): this.type = setEstimators(Array(value))

  /** @group setParam */
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = setEstimatorsParamMaps(Array(value))

  /** @group setParam */
  @Since("1.2.0")
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  @Since("1.2.0")
  def setNumFolds(value: Int): this.type = set(numFolds, value)

  /** @group setParam */
  @Since("2.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group getParam */
  override def getEstimatorParamMaps: Array[ParamMap] = $(estimatorsParamMaps).head

  /** @group getParam */
  override def getEstimator: Estimator[_] = $(estimators).head

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): CrossValidatorModel = {
    val schema = dataset.schema
    transformSchema(schema)
    val sparkSession = dataset.sparkSession
    val ests = $(estimators)
    val eval = $(evaluator)
    val epms = $(estimatorsParamMaps)
    val flatEpms = epms.flatten
    val numModels = flatEpms.length
    val metrics = new Array[Double](numModels)
    val modelIdxToEstIdx = epms.map(_.length).zipWithIndex.flatMap(idx => List.fill(idx._1)(idx._2))

    val splits = MLUtils.kFold(dataset.toDF.rdd, $(numFolds), $(seed))
    splits.zipWithIndex.foreach { case ((training, validation), splitIndex) =>
      val trainingDataset = sparkSession.createDataFrame(training, schema).cache()
      val validationDataset = sparkSession.createDataFrame(validation, schema).cache()
      // multi-model training
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")
      val models = ests.zip(epms).flatMap(estEpm => estEpm._1.fit(trainingDataset, estEpm._2).asInstanceOf[Seq[Model[_]]])

      trainingDataset.unpersist()
      var i = 0
      while (i < numModels) {
        // TODO: duplicate evaluator to take extra params from input
        val metric = eval.evaluate(models(i).transform(validationDataset, flatEpms(i)))
        logDebug(s"Got metric $metric for model trained with estimator ${ests(modelIdxToEstIdx(i))} and parameters ${flatEpms(i)}.")
        metrics(i) += metric
        i += 1
      }
      validationDataset.unpersist()
    }
    f2jBLAS.dscal(numModels, 1.0 / $(numFolds), metrics, 1)
    logInfo(s"Average cross-validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    logInfo(s"Best estimator:\n${ests(modelIdxToEstIdx(bestIndex))}")
    logInfo(s"Best set of parameters:\n${flatEpms(bestIndex)}")
    logInfo(s"Best cross-validation metric: $bestMetric.")
    val bestModel = ests(modelIdxToEstIdx(bestIndex)).fit(dataset, flatEpms(bestIndex)).asInstanceOf[Model[_]]
    copyValues(new CrossValidatorModel(uid, bestModel, metrics).setParent(this))
  }

  @Since("1.2.0")
  override def transformSchema(schema: StructType): StructType = {
    require($(estimators).length == $(estimatorsParamMaps).length, s"Number of estimators must match number of estimatorParamMaps")
    $(estimators).indices.map(idx => transformSchemaImpl(schema, idx))
      .foldLeft(schema)((acc, curr) => StructTypeMerge.merge(acc, curr))
  }

  protected def transformSchemaImpl(schema: StructType, idx: Int): StructType = {
    require($(estimatorsParamMaps)(idx).nonEmpty, s"Validator requires non-empty estimatorParamMaps")
    val firstEstimatorParamMap = $(estimatorsParamMaps)(idx).head
    val est = $(estimators)(idx)
    for (paramMap <- $(estimatorsParamMaps)(idx).tail) {
      est.copy(paramMap).transformSchema(schema)
    }
    est.copy(firstEstimatorParamMap).transformSchema(schema)
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): MultiCrossValidator = {
    val copied = defaultCopy(extra).asInstanceOf[MultiCrossValidator]
    if (copied.isDefined(estimators)) {
      copied.setEstimators(copied.getEstimators.map(_.copy(extra)))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }
}
