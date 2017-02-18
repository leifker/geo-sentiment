package com.github.leifker.spark.sentiment

import com.datastax.spark.connector.CassandraRow
import com.github.leifker.spark.AmazonReviewsTestContext
import com.github.leifker.spark.test.ITest
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import org.scalatest.tagobjects.Slow

/**
  * Created by dleifker on 2/16/17.
  */
class AmazonReviewsIT extends FlatSpec {
  val electronicsReviews: RDD[CassandraRow] = AmazonReviewsTestContext.amazonReviews.reviews.filter(_.getString("rootcategory") == "Electronics")
    .sample(false, 0.001)
    .cache()

  "Spark" should "be able to process text reviews of sample rows" taggedAs(ITest, Slow) in {
    electronicsReviews.foreach(row => NLPUtils.enchancedTokens(row.getString("reviewtext")))
  }

  it should "be able get at least a 500 sample" taggedAs(ITest, Slow) in {
    assert(electronicsReviews.count() >= 500)
  }
}
