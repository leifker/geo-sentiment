package com.github.leifker.cassandra.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Created by dleifker on 2/15/17.
 */
@Configuration
@Import(AmazonReviewsBeans.class)
public class AmazonReviewLoaderBeans {
  @Bean(name = AmazonReviewsBeans.AMAZON_REVIEWS)
  public KeyspaceConfig amazonReviewsKeyspaceConfig() {
    return new KeyspaceConfig("amazon_reviews");
  }

  @Bean
  public CassandraConfig cassandraConfig() {
    return new CassandraConfig();
  }
}
