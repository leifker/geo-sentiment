package com.github.leifker.cassandra.sentiment;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;
import com.github.leifker.cassandra.CassandraModel;

import java.util.Objects;

/**
 * Created by dleifker on 2/14/17.
 */
@Table(name = "amazon_category_by_productid",
    readConsistency = "ONE",
    writeConsistency = "LOCAL_QUORUM")
public class AmazonCategory extends CassandraModel<String> {
  @PartitionKey(0)
  private String productId;
  @ClusteringColumn(0)
  private String rootCategory = NULL_STRING;
  @ClusteringColumn(1)
  private String categoryPath = NULL_STRING;

  private String value = NULL_STRING;

  public AmazonCategory() {
  }

  @Transient
  @Override
  public String getPartitionKey() {
    return getProductId();
  }

  @Transient
  @Override
  public void setPartitionKey(String key) {
    setProductId(key);
  }

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public String getRootCategory() {
    return rootCategory;
  }

  public void setRootCategory(String rootCategory) {
    this.rootCategory = rootCategory;
  }

  public String getCategoryPath() {
    return categoryPath;
  }

  public void setCategoryPath(String categoryPath) {
    this.categoryPath = categoryPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AmazonCategory that = (AmazonCategory) o;
    return Objects.equals(productId, that.productId) &&
        Objects.equals(rootCategory, that.rootCategory) &&
        Objects.equals(categoryPath, that.categoryPath) &&
        Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(productId, rootCategory, categoryPath, value);
  }

  @Override
  public String toString() {
    return "AmazonCategory{" +
        "productId='" + productId + '\'' +
        ", rootCategory='" + rootCategory + '\'' +
        ", categoryPath='" + categoryPath + '\'' +
        ", value='" + value + '\'' +
        '}';
  }
}
