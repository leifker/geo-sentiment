package com.github.leifker.spark

import com.github.leifker.cassandra.config.{CassandraConfig, KeyspaceConfig}
import com.github.leifker.spark.config.CassandraSparkConfig
import com.github.leifker.spark.sentiment.AmazonReviews

/**
  * Created by dleifker on 2/16/17.
  */
object AmazonReviewsTestContext {
  val cassandraSparkConfig = CassandraSparkConfig(new CassandraConfig("localhost"), "local[4]")
  val amazonReviewsKeyspace = new KeyspaceConfig("amazon_reviews")
  val amazonReviews = new AmazonReviews(cassandraSparkConfig, amazonReviewsKeyspace, "IntegrationTest")
}
