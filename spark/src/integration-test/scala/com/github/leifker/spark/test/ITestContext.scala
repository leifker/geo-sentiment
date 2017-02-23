package com.github.leifker.spark.test

import com.github.leifker.cassandra.config.{CassandraConfig, KeyspaceConfig}
import com.github.leifker.spark.config.CassandraSparkConfig

/**
  * Created by dleifker on 2/16/17.
  */
object ITestContext {
  val localConfig = CassandraSparkConfig(new CassandraConfig("alpha,epsilon,eta,delta"), "local[4]")
  val clusterConfig = CassandraSparkConfig(new CassandraConfig("alpha,epsilon,eta,delta"), "spark://zeta:7077", Seq("spark/build/libs/spark-0.0.1-SNAPSHOT-shadow.jar"))
  val amazonReviewsKeyspace = new KeyspaceConfig("amazon_reviews")
}
