package com.github.leifker.cassandra.config;

import com.datastax.driver.mapping.MappingManager;
import com.github.leifker.cassandra.SchemaManager;
import com.github.leifker.cassandra.sentiment.AmazonReviewByCategoryDao;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * Created by dleifker on 2/14/17.
 */
@Configuration
@Import(CassandraBeans.class)
public class AmazonReviewsBeans {
  public static final String AMAZON_REVIEWS = "amazonReviews";


  @Bean
  @Inject
  public AmazonReviewByCategoryDao amazonReviewByCategoryDao(SchemaManager schemaManager, @Named(AMAZON_REVIEWS) KeyspaceConfig config) {
    schemaManager.createIfNotExists(config);

    MappingManager mappingManager = new MappingManager(schemaManager.getKeyspaceSession(config));
    AmazonReviewByCategoryDao dao = new AmazonReviewByCategoryDao(mappingManager);
    return dao;
  }
}
