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
public interface AmazonCategoryAccessor {
  @Query("SELECT * FROM amazon_category_by_productid WHERE productid = :productid")
  @QueryParameters(consistency = "ONE")
  Statement findByPartitionKey(@Param("productid") String productid);

  @Query("SELECT productid, rootcategory FROM amazon_category_by_productid WHERE productid = :productid LIMIT 1")
  @QueryParameters(consistency = "ONE")
  Statement findRootCategoryByProductId(@Param("productid") String productid);

  @Query("DELETE FROM amazon_category_by_productid WHERE productid = :productid")
  @QueryParameters(consistency = "ONE")
  Statement deleteByPartitionkey(@Param("productid") String productid);
}
