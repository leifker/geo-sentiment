package com.github.leifker.spark.sentiment

import com.datastax.spark.connector._
import com.github.leifker.cassandra.config.KeyspaceConfig
import com.github.leifker.spark.CassandraSparkContext
import com.github.leifker.spark.config.CassandraSparkConfig

/**
  * Created by dleifker on 2/16/17.
  */
class AmazonReviews(cassandraConfig: CassandraSparkConfig, keyspaceConfig: KeyspaceConfig, appName: String = "AmazonReviews") extends CassandraSparkContext(cassandraConfig, appName) {
  lazy val reviews = session.sparkContext.cassandraTable(keyspaceConfig.getName, "amazon_reviews_by_category")
  lazy val categories = session.sparkContext.cassandraTable(keyspaceConfig.getName, "amazon_category_by_productid")
}
