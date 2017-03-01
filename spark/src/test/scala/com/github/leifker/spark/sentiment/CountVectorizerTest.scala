package com.github.leifker.spark.sentiment

import com.github.leifker.cassandra.config.CassandraConfig
import com.github.leifker.spark.CassandraSparkContext
import com.github.leifker.spark.config.CassandraSparkConfig
import com.github.leifker.spark.sentiment.test.UnitTest
import org.apache.spark.ml.feature.CountVectorizer
import org.scalatest.FlatSpec

/**
  * Tests assumptions on CountVectorizer
  */
class CountVectorizerTest extends FlatSpec {

  val conf = CassandraSparkConfig(new CassandraConfig(), "local[4]")
  val context = new CassandraSparkContext(conf, "CountVectorizerTest")
  val sqlContext = context.session.sqlContext

  val df = sqlContext.createDataFrame(Seq(
    (0, Array("bad", "horrible", "horrible")),
    (1, Array("good", "excellent", "excellent")),
    (2, Array("good")),
    (3, Array("bad"))
  )).toDF("id", "words")

  "CountVectorizer" should "apply min document frequency" taggedAs(UnitTest) in {
    val countVectorizer = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinDF(2)

    assert(countVectorizer.fit(df).vocabulary.toSet == Set("bad", "good"))
  }

  it should "apply min term frequency" taggedAs(UnitTest) in {
    val countVectorizer = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinTF(2)

    assert(countVectorizer.fit(df).vocabulary.toSet == Set("excellent", "horrible", "bad", "good"))
  }
}
