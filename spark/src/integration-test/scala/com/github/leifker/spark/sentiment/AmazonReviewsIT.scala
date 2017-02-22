package com.github.leifker.spark.sentiment

import com.github.leifker.spark.AmazonReviewsTestContext
import com.github.leifker.spark.test.ITest
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{Binarizer, CountVectorizer, CountVectorizerModel, HashingTF}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.FlatSpec
import org.scalatest.tagobjects.Slow

/**
  * Created by dleifker on 2/16/17.
  */
class AmazonReviewsIT extends FlatSpec {
  val reviews = AmazonReviewsTestContext.amazonReviews.extremePosNegElectronicsReviews
    .limit(100000)
    .coalesce(12)
    .cache()
  val sampleReviews: Dataset[Row] = reviews.sample(false, 0.01)

  "Spark" should "be able to process text reviews of sample rows" taggedAs(ITest, Slow) in {
    sampleReviews.foreach(row => NLPUtils.enhancedTokens(row.getAs[String]("text")))
  }

  it should "be able get at least a 500 sample" taggedAs(ITest, Slow) in {
    assert(sampleReviews.count() >= 500)
  }

  it should "be able to tokenize" taggedAs(ITest, Slow) in {
    val tokenizer = new ReviewTokenizer().setInputCol("text").setOutputCol("words")
    val tokenized = tokenizer.transform(reviews)
    assert(tokenized.select("words", "score").take(1000).length == 1000)
  }

  it should "vectorize" taggedAs(ITest, Slow) in {
    val tokenizer = new ReviewTokenizer().setInputCol("text").setOutputCol("words")
    val tokenized = tokenizer.transform(reviews.limit(1000))
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(500)
      .setMinDF(10)
      .fit(tokenized)

    cvModel.transform(tokenized).select("features").show()
  }

  it should "pipeline" taggedAs(ITest, Slow) in {
    val tokenizer = new ReviewTokenizer().setInputCol("text").setOutputCol("words")
    val binarizer: Binarizer = new Binarizer()
      .setInputCol("score")
      .setOutputCol("label")
      .setThreshold(3.0)
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(5)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, binarizer, hashingTF, lr))

    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 1000))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)  // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(reviews)

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    cvModel.transform(sampleReviews).show(500)
  }
}
