package com.github.leifker.cassandra.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.validation.constraints.NotNull;

/**
 * Created by dleifker on 2/14/17.
 */
public class CassandraConfig {
  @JsonProperty
  @NotNull
  private String seedHosts = "localhost";

  /**
   * Default
   */
  public CassandraConfig() {
  }

  @JsonCreator
  public CassandraConfig(@JsonProperty("seedHosts") String seedHosts) {
    this.seedHosts = Preconditions.checkNotNull(seedHosts);
  }

  public String getSeedHosts() {
    return seedHosts;
  }

  public void setSeedHosts(String seedHosts) {
    this.seedHosts = seedHosts;
  }
}
