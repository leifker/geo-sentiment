package com.github.leifker.cassandra.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions;
import com.github.leifker.cassandra.SchemaManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.inject.Inject;

/**
 * Created by dleifker on 2/14/17.
 */
@Configuration
public class CassandraBeans {
  @Bean
  @Inject
  public Cluster cassandraCluster(CassandraConfig config) {
    Cluster cluster = Cluster.builder()
        .addContactPoint(config.getSeedHosts())
        .build();

    cluster.getConfiguration().getProtocolOptions().setCompression(ProtocolOptions.Compression.LZ4);

    return cluster;
  }

  @Bean
  @Inject
  public SchemaManager schemaManager(Cluster cluster) {
    return new SchemaManager(cluster);
  }
}
