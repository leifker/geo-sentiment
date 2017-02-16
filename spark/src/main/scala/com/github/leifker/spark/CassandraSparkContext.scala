package com.github.leifker.spark

import com.github.leifker.spark.config.CassandraSparkConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by dleifker on 2/16/17.
  */
class CassandraSparkContext(config: CassandraSparkConfig, appName: String = "Sentiment") {
   val sparkConf = new SparkConf(true)
      .setMaster(config.sparkMaster)
      .setAppName(appName)
      .setAll(config.sparkSettings)

  val session = SparkSession.builder()
      .master(config.sparkMaster)
      .appName(appName)
      .config(sparkConf)
      .getOrCreate()
}
