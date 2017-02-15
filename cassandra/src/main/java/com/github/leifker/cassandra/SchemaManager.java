package com.github.leifker.cassandra;

import com.datastax.driver.core.*;
import com.github.leifker.cassandra.config.KeyspaceConfig;
import com.google.common.base.Preconditions;

/**
 * Created by dleifker on 2/14/17.
 */
public class SchemaManager {
  private final Session session;

  public SchemaManager(Cluster cluster) {
    this.session = Preconditions.checkNotNull(cluster).newSession();
  }

  public Session getKeyspaceSession(KeyspaceConfig config) {
    return session.getCluster().connect(config.getName());
  }

  public void createIfNotExists(KeyspaceConfig config) {
    session.execute(config.getDefaultSchema());
  }

  public void drop(KeyspaceConfig config) {
    Statement dropKeyspace = new SimpleStatement(String.format("DROP KEYSPACE IF EXISTS %s", config.getName()));
    session.execute(dropKeyspace);
  }
}
