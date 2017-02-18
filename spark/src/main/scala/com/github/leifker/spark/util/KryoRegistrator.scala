package com.github.leifker.spark.util

import com.datastax.spark.connector.{CassandraRow, CassandraRowMetadata}
import com.esotericsoftware.kryo.Kryo
import com.github.leifker.cassandra.config.{CassandraConfig, KeyspaceConfig}
import org.apache.spark.serializer.{KryoRegistrator => SparkKryoRegistrator}

/**
  * Created by dleifker on 2/16/17.
  */
class KryoRegistrator extends SparkKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[CassandraConfig])
    kryo.register(classOf[KeyspaceConfig])
    kryo.register(classOf[Array[CassandraRow]])
    kryo.register(classOf[CassandraRow])
    kryo.register(classOf[CassandraRowMetadata])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[Array[Object]])
  }
}
