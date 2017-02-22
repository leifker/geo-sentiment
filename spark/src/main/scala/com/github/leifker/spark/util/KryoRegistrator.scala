package com.github.leifker.spark.util

import com.datastax.spark.connector.{CassandraRow, CassandraRowMetadata}
import com.esotericsoftware.kryo.Kryo
import com.github.leifker.cassandra.config.{CassandraConfig, KeyspaceConfig}
import com.github.leifker.spark.sentiment.Review
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.serializer.{KryoRegistrator => SparkKryoRegistrator}

/**
  * Created by dleifker on 2/16/17.
  */
class KryoRegistrator extends SparkKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(Class.forName("scala.math.Ordering$$anon$9"))
    kryo.register(Class.forName("scala.math.Ordering$$anonfun$by$1"))
    kryo.register(Class.forName("org.apache.spark.ml.feature.CountVectorizer$$anonfun$6"))
    kryo.register(Class.forName("org.apache.spark.ml.classification.MultiClassSummarizer"))
    kryo.register(Class.forName("org.apache.spark.ml.classification.LogisticAggregator"))
    kryo.register(Class.forName("org.apache.spark.mllib.evaluation.binary.BinaryLabelCounter"))
    kryo.register(Class.forName("[Lorg.apache.spark.mllib.evaluation.binary.BinaryLabelCounter;"))
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(Class.forName("[[B"))
    kryo.register(classOf[Class[_]])
    kryo.register(scala.math.Ordering.Long.getClass)
    kryo.register(classOf[MultivariateOnlineSummarizer])
    kryo.register(classOf[Review])
    kryo.register(classOf[CassandraConfig])
    kryo.register(classOf[KeyspaceConfig])
    kryo.register(classOf[Array[CassandraRow]])
    kryo.register(classOf[CassandraRow])
    kryo.register(classOf[CassandraRowMetadata])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[Array[Object]])
    kryo.register(classOf[Array[Double]])
  }
}
