package com.github.leifker.spark.sentiment

import com.github.leifker.spark.AmazonReviewsTestContext
import com.github.leifker.spark.test.ITest
import org.scalatest.FlatSpec

/**
  * Created by dleifker on 2/16/17.
  */
class AmazonReviewsIT extends FlatSpec {
  "Spark" should "be able to read reviews by category" taggedAs(ITest) in {
    val rdd = AmazonReviewsTestContext.amazonReviews.reviews.filter(_.getString("rootcategory") == "Electronics").take(1000)
    assert(rdd.length == 1000)
  }
}
