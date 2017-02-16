package com.github.leifker.cassandra.sentiment;

import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.driver.mapping.annotations.QueryParameters;

/**
 * Created by dleifker on 2/15/17.
 */
@Accessor
public interface AmazonReviewAccessor {
  @Query("SELECT * FROM amazon_reviews_by_category WHERE rootcategory = :rootcategory AND score = :score AND productid = :productid")
  @QueryParameters(consistency = "ONE")
  Statement findByPartitionKey(@Param("rootcategory") String category, @Param("score") Integer score, @Param("productid") String productid);

  @Query("DELETE FROM amazon_reviews_by_category WHERE rootcategory = :rootcategory AND score = :score AND productid = :productid")
  @QueryParameters(consistency = "ONE")
  Statement deleteByPartitionkey(@Param("rootcategory") String category, @Param("score") Integer score, @Param("productid") String productid);
}
