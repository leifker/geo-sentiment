package com.github.leifker.spark.test

import com.github.leifker.cassandra.config.{CassandraConfig, KeyspaceConfig}
import com.github.leifker.spark.config.CassandraSparkConfig

/**
  * Created by dleifker on 2/16/17.
  */
object ITestContext {
  val localConfig = CassandraSparkConfig(new CassandraConfig("localhost"), "local[4]")
  val clusterConfig = CassandraSparkConfig(new CassandraConfig("localhost"), "spark://zeta")
  val amazonReviewsKeyspace = new KeyspaceConfig("amazon_reviews")
}
