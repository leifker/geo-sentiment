package com.github.leifker.spark.sentiment

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.github.leifker.cassandra.config.KeyspaceConfig
import com.github.leifker.spark.CassandraSparkContext
import com.github.leifker.spark.config.CassandraSparkConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by dleifker on 2/16/17.
  */
case class AmazonReviews(cassandraConfig: CassandraSparkConfig, keyspaceConfig: KeyspaceConfig, appName: String = "AmazonReviews") extends CassandraSparkContext(cassandraConfig, appName) {
  lazy val reviews: CassandraTableScanRDD[CassandraRow] = session.sparkContext.cassandraTable(keyspaceConfig.getName, "amazon_reviews_by_category_score")
  lazy val categories: CassandraTableScanRDD[CassandraRow] = session.sparkContext.cassandraTable(keyspaceConfig.getName, "amazon_category_by_productid")

  lazy val electronicsCategories = Seq("Electronics", "All Electronics", "Car Electronics", "Computers", "Camera & Photo", "Cell Phones & Accessories", "Video Games",
    "GPS & Navigation", "MP3 Players & Accessories", "Software")

  lazy val electronicsReviews: Seq[CassandraTableScanRDD[CassandraRow]] = electronicsCategories
    .map(reviews.select("rootcategory", "score", "reviewtext").where("rootcategory = ?", _))

  import session.sqlContext.implicits._

  /*
     159928 1.0
     100422 2.0
     116751 3.0
     137152 4.0
     151993 5.0
   */
  lazy val fiveStarElectronics: DataFrame = union(electronicsReviews.map(_.where("score = ?", 5)))
    .map(r => Review(r.getInt("score"), r.getString("reviewtext")))
    .toDF()
  lazy val fourStarElectronics: DataFrame = union(electronicsReviews.map(_.where("score = ?", 4)))
    .map(r => Review(r.getInt("score"), r.getString("reviewtext")))
    .toDF()
  lazy val threeStarElectronics: DataFrame = union(electronicsReviews.map(_.where("score = ?", 3)))
    .map(r => Review(r.getInt("score"), r.getString("reviewtext")))
    .toDF()
  lazy val twoStarElectronics: DataFrame = union(electronicsReviews.map(_.where("score = ?", 2)))
    .map(r => Review(r.getInt("score"), r.getString("reviewtext")))
    .toDF()
  lazy val oneStarElectronics: DataFrame = union(electronicsReviews.map(_.where("score = ?", 1)))
    .map(r => Review(r.getInt("score"), r.getString("reviewtext")))
    .toDF()

  private def union(queries: Seq[RDD[CassandraRow]]): RDD[CassandraRow] = {
    queries.foldLeft(null: RDD[CassandraRow])({
      case (a, b) if a == null => b
      case (a, b)  => a.union(b)
    })
  }
}
