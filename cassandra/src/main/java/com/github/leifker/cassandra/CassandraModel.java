package com.github.leifker.cassandra;

import com.datastax.driver.mapping.annotations.Transient;

/**
 * Created by dleifker on 2/14/17.
 * @param <PK> partition key
 */
abstract public class CassandraModel<PK> {
  @Transient
  protected final String NULL_STRING = "";
  @Transient
  protected final Long NULL_LONG = 0L;
  @Transient
  protected final Integer NULL_INTEGER = 0;

  /**
   * Returns the partition key
   * @return the partition key
   */
  @Transient
  abstract public PK getPartitionKey();

  /**
   * Sets the partition key
   * @param key to set
   */
  @Transient
  abstract public void setPartitionKey(PK key);
}
