package com.github.leifker.spark.sentiment

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.github.leifker.cassandra.config.KeyspaceConfig
import com.github.leifker.spark.CassandraSparkContext
import com.github.leifker.spark.config.CassandraSparkConfig
import org.apache.spark.rdd.RDD

/**
  * Created by dleifker on 2/16/17.
  */
case class AmazonReviews(cassandraConfig: CassandraSparkConfig, keyspaceConfig: KeyspaceConfig, appName: String = "AmazonReviews") extends CassandraSparkContext(cassandraConfig, appName) {
  lazy val reviews: CassandraTableScanRDD[CassandraRow] = session.sparkContext.cassandraTable(keyspaceConfig.getName, "amazon_reviews_by_category")
  lazy val categories: CassandraTableScanRDD[CassandraRow] = session.sparkContext.cassandraTable(keyspaceConfig.getName, "amazon_category_by_productid")

  lazy val electronicsReviews: RDD[CassandraRow] = reviews.select("rootcategory", "score", "reviewtext").filter(_.getString("rootcategory") == "Electronics")

  import session.sqlContext.implicits._
  lazy val positiveElectronicsReviews = electronicsReviews
    .filter(r => Set(4, 5).contains(r.getInt("score")))
    .map(r => Review(r.getInt("score"), r.getString("reviewtext")))
    .toDF()
  lazy val negativeElectronicsReviews = electronicsReviews
    .filter(r => Set(1, 2).contains(r.getInt("score")))
    .map(r => Review(r.getInt("score"), r.getString("reviewtext")))
    .toDF()
  lazy val extremePosNegElectronicsReviews =  electronicsReviews
    .filter(r => Set(1, 5).contains(r.getInt("score")))
    .map(r => Review(r.getInt("score"), r.getString("reviewtext")))
    .toDF()
}
