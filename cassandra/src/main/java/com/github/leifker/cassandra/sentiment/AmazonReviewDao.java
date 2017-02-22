package com.github.leifker.cassandra.sentiment;

import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.github.leifker.cassandra.AbstractDao;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Created by dleifker on 2/14/17.
 */
public class AmazonReviewDao extends AbstractDao<Pair<String, Integer>, AmazonReview> {
  private final Mapper<AmazonReview> mapper;
  private final AmazonReviewAccessor accessor;

  public AmazonReviewDao(MappingManager mappingManager) {
    super(mappingManager.getSession());
    this.mapper = Preconditions.checkNotNull(mappingManager).mapper(AmazonReview.class);
    this.accessor = mappingManager.createAccessor(AmazonReviewAccessor.class);
  }

  @Override
  protected Statement deleteByPartitionKey(Pair<String, Integer> key) {
    return accessor.deleteByPartitionkey(key.getLeft(), key.getRight());
  }

  @Override
  protected Statement findByPartitionkey(Pair<String, Integer> key) {
    return accessor.findByPartitionKey(key.getLeft(), key.getRight());
  }

  @Override
  protected Mapper<AmazonReview> getMapper() {
    return mapper;
  }

  @Override
  public String getDefaultSchema() {
    return "CREATE TABLE IF NOT EXISTS amazon_reviews_by_category_score (\n" +
        "    rootcategory text,\n" +
        "    score int,\n" +
        "    productid text,\n" +
        "    time bigint,\n" +
        "    userid text,\n" +
        "    title text,\n" +
        "    price text,\n" +
        "    profilename text,\n" +
        "    helpfulness text,\n" +
        "    summary text,\n" +
        "    reviewtext text,\n" +
        "    PRIMARY KEY ((rootcategory, score), time, productid, userid, title, price, profilename, helpfulness, summary)\n" +
        ") WITH COMPACT STORAGE\n" +
        "    AND CLUSTERING ORDER BY (time DESC, productid ASC, userid ASC, title ASC, price ASC, profilename ASC, helpfulness ASC, summary ASC)\n" +
        "    AND comment = 'Amazon Review Data'\n" +
        "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}\n" +
        "    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};";
  }
}
