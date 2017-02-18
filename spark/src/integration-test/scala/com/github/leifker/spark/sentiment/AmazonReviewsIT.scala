package com.github.leifker.spark.sentiment

import com.github.leifker.spark.AmazonReviewsTestContext
import com.github.leifker.spark.test.ITest
import org.scalatest.FlatSpec
import org.scalatest.tagobjects.Slow

/**
  * Created by dleifker on 2/16/17.
  */
class AmazonReviewsIT extends FlatSpec {
  val electronicsReviews = AmazonReviewsTestContext.amazonReviews.reviews.filter(_.getString("rootcategory") == "Electronics")
    .sample(false, 0.001)
    .cache()

  "Spark" should "be able to read reviews by category" taggedAs(ITest, Slow) in {
    val rdd = electronicsReviews.take(1000)
    assert(rdd.length == 1000)
  }

  it should "be able get a large sample" taggedAs(ITest, Slow) in {
    assert(electronicsReviews.sample(false, 0.001).count() >= 500)
  }
}
