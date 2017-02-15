package com.github.leifker.cassandra.sentiment;

import com.datastax.driver.mapping.annotations.*;
import com.github.leifker.cassandra.CassandraModel;
import org.apache.commons.lang3.tuple.Triple;

import java.time.Instant;
import java.util.Objects;

/**
 * Created by dleifker on 2/14/17.
 */
@Table(name = "amazon_reviews_by_category",
    readConsistency = "ONE",
    writeConsistency = "LOCAL_QUORUM")
public class AmazonReviewByCategory extends CassandraModel<Triple<String, Integer, String>> {
  @PartitionKey(0)
  private String category = NULL_STRING;
  @PartitionKey(1)
  private Integer score = NULL_INTEGER;
  @PartitionKey(2)
  private String productId = NULL_STRING;
  @ClusteringColumn(0)
  private Long time = NULL_LONG;
  @ClusteringColumn(1)
  private String title = NULL_STRING;
  @ClusteringColumn(2)
  private String price = NULL_STRING;
  @ClusteringColumn(3)
  private String userId = NULL_STRING;
  @ClusteringColumn(4)
  private String profileName = NULL_STRING;
  @ClusteringColumn(5)
  private String helpfulness = NULL_STRING;
  @ClusteringColumn(6)
  private String summary = NULL_STRING;

  private String reviewText;

  public AmazonReviewByCategory() {
  }

  @Transient
  @Override
  public Triple<String, Integer, String> getPartitionKey() {
    return Triple.of(getCategory(), getScore(), getProductId());
  }

  @Transient
  @Override
  public void setPartitionKey(Triple<String, Integer, String> key) {
    setCategory(key.getLeft());
    setScore(key.getMiddle());
    setProductId(key.getRight());
  }

  public String getCategory() {
    return category;
  }

  public void setCategory(String category) {
    this.category = category;
  }

  public Integer getScore() {
    return score;
  }

  public void setScore(Integer score) {
    this.score = score;
  }

  public String getProductId() {
    return productId;
  }

  public void setProductId(String productId) {
    this.productId = productId;
  }

  public Long getTime() {
    return time;
  }

  public void setTime(Long time) {
    this.time = time;
  }

  @Transient
  public Instant getTimeInstant() {
    return Instant.ofEpochSecond(getTime());
  }

  @Transient
  public void setTimeInstant(Instant time) {
    setTime(time.getEpochSecond());
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getPrice() {
    return price;
  }

  public void setPrice(String price) {
    this.price = price;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getProfileName() {
    return profileName;
  }

  public void setProfileName(String profileName) {
    this.profileName = profileName;
  }

  public String getHelpfulness() {
    return helpfulness;
  }

  public void setHelpfulness(String helpfulness) {
    this.helpfulness = helpfulness;
  }

  public String getSummary() {
    return summary;
  }

  public void setSummary(String summary) {
    this.summary = summary;
  }

  public String getReviewText() {
    return reviewText;
  }

  public void setReviewText(String reviewText) {
    this.reviewText = reviewText;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AmazonReviewByCategory that = (AmazonReviewByCategory) o;
    return Objects.equals(category, that.category) &&
        Objects.equals(score, that.score) &&
        Objects.equals(productId, that.productId) &&
        Objects.equals(time, that.time) &&
        Objects.equals(title, that.title) &&
        Objects.equals(price, that.price) &&
        Objects.equals(userId, that.userId) &&
        Objects.equals(profileName, that.profileName) &&
        Objects.equals(helpfulness, that.helpfulness) &&
        Objects.equals(summary, that.summary) &&
        Objects.equals(reviewText, that.reviewText);
  }

  @Override
  public int hashCode() {
    return Objects.hash(category, score, productId, time, title, price, userId, profileName, helpfulness, summary, reviewText);
  }

  @Override
  public String toString() {
    return "AmazonReviewByCategory{" +
        "category='" + category + '\'' +
        ", score=" + score +
        ", productId='" + productId + '\'' +
        ", time=" + time +
        ", title='" + title + '\'' +
        ", price='" + price + '\'' +
        ", userId='" + userId + '\'' +
        ", profileName='" + profileName + '\'' +
        ", helpfulness='" + helpfulness + '\'' +
        ", summary='" + summary + '\'' +
        ", reviewText='" + reviewText + '\'' +
        '}';
  }
}
