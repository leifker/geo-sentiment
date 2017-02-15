package com.github.leifker.cassandra.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by dleifker on 2/14/17.
 */
@Configuration
public class CassandraTestBeans {

  @Bean(name = AmazonReviewsBeans.AMAZON_REVIEWS)
  public KeyspaceConfig amazonReviewsKeyspaceConfig() {
    return new KeyspaceConfig("amazon_reviews_test");
  }

  @Bean
  public CassandraConfig cassandraConfig() {
    return new CassandraConfig("localhost");
  }
}
