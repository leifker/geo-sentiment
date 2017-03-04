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
  val reviews: CassandraTableScanRDD[CassandraRow] = session.sparkContext.cassandraTable(keyspaceConfig.getName, "amazon_reviews_by_category_score")
  val categories: CassandraTableScanRDD[CassandraRow] = session.sparkContext.cassandraTable(keyspaceConfig.getName, "amazon_category_by_productid")

  val categoriesMap: Map[String, Set[String]] = Map("Electronics" -> Set("Electronics", "All Electronics", "Car Electronics", "Computers", "Camera & Photo", "Cell Phones & Accessories",
    "Video Games", "GPS & Navigation", "MP3 Players & Accessories", "Software"))
  val scores: Seq[Int] = Range.inclusive(1, 5)

  val categoryScoreMap: Map[String, Map[Int, Seq[RDD[CassandraRow]]]] = {
    categoriesMap.map(entry =>
      entry._1 -> entry._2.flatMap(subCategory =>
        scores.map(score =>
          score -> reviews.select("rootcategory", "score", "reviewtext")
            .where("rootcategory = ? AND score = ?", subCategory, score)
        )
      ).toSeq.groupBy(_._1).mapValues(_.map(_._2))
    )
  }

  import session.sqlContext.implicits._

  /*
     159928 1.0
     100422 2.0
     116751 3.0
     137152 4.0
     151993 5.0
   */
  lazy val fiveStarElectronics: DataFrame = unionAll(categoryScoreMap("Electronics")(5)).toDF()
  lazy val fourStarElectronics: DataFrame = unionAll(categoryScoreMap("Electronics")(4)).toDF()
  lazy val threeStarElectronics: DataFrame = unionAll(categoryScoreMap("Electronics")(3)).toDF()
  lazy val twoStarElectronics: DataFrame = unionAll(categoryScoreMap("Electronics")(2)).toDF()
  lazy val oneStarElectronics: DataFrame = unionAll(categoryScoreMap("Electronics")(1)).toDF()

  private def unionAll(queries: Seq[RDD[CassandraRow]]): RDD[Review] = {
    queries.foldLeft(null: RDD[CassandraRow])({
      case (a, b) if a == null => b
      case (a, b)  => a.union(b)
    }).map(r => Review(r.getInt("score"), r.getString("reviewtext"), r.getString("productid")))
  }
}
