package com.github.leifker.spark.config

import com.github.leifker.cassandra.config.CassandraConfig

/**
  * Created by dleifker on 2/16/17.
  */
case class CassandraSparkConfig(cassandraConfig: CassandraConfig, sparkMaster: String, sparkSettings: List[Tuple2[String, String]], sparkJars: Seq[String])

object CassandraSparkConfig {
  def apply(cassandraConfig: CassandraConfig, sparkMaster: String = "local[2]"): CassandraSparkConfig = {
    CassandraSparkConfig(cassandraConfig, sparkMaster, Seq())
  }

  def apply(cassandraConfig: CassandraConfig, sparkMaster: String, sparkJars: Seq[String]): CassandraSparkConfig = {
    new CassandraSparkConfig(
      cassandraConfig,
      sparkMaster,
      List(
        ("spark.cassandra.connection.host", cassandraConfig.getSeedHosts),
        ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
        ("spark.kryo.registrationRequired", "true"),
        ("spark.kryo.registrator", "com.github.leifker.spark.util.KryoRegistrator")
        /*,
        ("spark.driver.userClassPathFirst", "true"),
        ("spark.executor.userClassPathFirst", "true") */
      ),
      sparkJars
    )
  }
}
