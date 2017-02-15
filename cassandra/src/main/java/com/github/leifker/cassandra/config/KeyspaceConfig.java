package com.github.leifker.cassandra.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Created by dleifker on 2/15/17.
 */
public class KeyspaceConfig {
  @JsonProperty
  @NotNull
  private String name;

  @JsonProperty
  @Min(1)
  private Integer replication = 2;

  @JsonCreator
  public KeyspaceConfig(@JsonProperty("name") String name) {
    this.name = Preconditions.checkNotNull(name);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Integer getReplication() {
    return replication;
  }

  public void setReplication(Integer replication) {
    this.replication = replication;
  }

  @JsonIgnore
  public String getDefaultSchema() {
    return String.format(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '%d'} AND durable_writes = true;",
        name, replication);
  }
}
