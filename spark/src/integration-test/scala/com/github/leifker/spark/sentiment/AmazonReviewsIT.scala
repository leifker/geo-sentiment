package com.github.leifker.spark.sentiment

import com.github.leifker.spark.test.{ITest, ITestContext}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.FlatSpec
import org.scalatest.tagobjects.Slow

/**
  * Created by dleifker on 2/16/17.
  */
class AmazonReviewsIT extends FlatSpec {
  val amazonReviews = AmazonReviews(ITestContext.localConfig, ITestContext.amazonReviewsKeyspace, "IntegrationTest")
  val oneStarReviews = amazonReviews.oneStarElectronics
    .sample(false, 0.2)
    .cache()
  val fiveStarReviews = amazonReviews.fiveStarElectronics
    .sample(false, 0.2)
    .cache()
  val sampleReviews: Dataset[Row] = amazonReviews.oneStarElectronics.sample(false, 0.007)
    .union(amazonReviews.fiveStarElectronics.sample(false, 0.007))

  "Spark" should "be able to process text reviews of sample rows" taggedAs(ITest, Slow) in {
    val tokenizer = new ReviewTokenizer()
    sampleReviews.foreach(row => tokenizer.transform(row.getAs[String]("text")))
  }

  it should "be able get at least a 500 sample" taggedAs(ITest, Slow) in {
    assert(sampleReviews.count() >= 1000)
  }

  it should "be able to tokenize" taggedAs(ITest, Slow) in {
    val tokenizer = new ReviewTokenizer().setInputCol("text").setOutputCol("words")
    val tokenized = tokenizer.transform(oneStarReviews)
    assert(tokenized.select("words", "score").take(1000).length == 1000)
  }

  it should "vectorize" taggedAs(ITest, Slow) in {
    val tokenizer = new ReviewTokenizer().setInputCol("text").setOutputCol("words")
    val tokenized = tokenizer.transform(oneStarReviews.limit(1000))
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(500)
      .setMinDF(10)
      .fit(tokenized)

    cvModel.transform(tokenized).select("features").show()
  }
}
