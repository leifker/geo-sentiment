package com.github.leifker.cassandra.sentiment;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.github.leifker.cassandra.AbstractDao;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by dleifker on 2/14/17.
 */
public class AmazonCategoryDao extends AbstractDao<String, AmazonCategory> {
  private final Mapper<AmazonCategory> mapper;
  private final AmazonCategoryAccessor accessor;

  public AmazonCategoryDao(MappingManager mappingManager) {
    super(mappingManager.getSession());
    this.mapper = Preconditions.checkNotNull(mappingManager).mapper(AmazonCategory.class);
    this.accessor = mappingManager.createAccessor(AmazonCategoryAccessor.class);
  }

  public Map<String, String> findRootCategories(Set<String> productIds) throws Exception {
    List<ResultSetFuture> futures = new ArrayList<>();
    productIds.forEach(key -> futures.add(getSession().executeAsync(accessor.findRootCategoryByProductId(key))));
    return StreamSupport.stream(Iterables.concat(executeFutures(futures, null).stream()
        .map(getMapper()::map).collect(Collectors.toList())).spliterator(), false)
        .collect(Collectors.toMap(AmazonCategory::getProductId, AmazonCategory::getRootCategory));
  }

  @Override
  protected Statement deleteByPartitionKey(String key) {
    return accessor.deleteByPartitionkey(key);
  }

  @Override
  protected Statement findByPartitionkey(String key) {
    return accessor.findByPartitionKey(key);
  }

  @Override
  protected Mapper<AmazonCategory> getMapper() {
    return mapper;
  }

  @Override
  public String getDefaultSchema() {
    return "CREATE TABLE IF NOT EXISTS amazon_category_by_productid (\n" +
        "    productid text,\n" +
        "    rootcategory text,\n" +
        "    categorypath text,\n" +
        "    value text,\n" +
        "    PRIMARY KEY (productid, rootcategory, categorypath)\n" +
        ") WITH COMPACT STORAGE\n" +
        "    AND CLUSTERING ORDER BY (rootcategory ASC, categorypath ASC)\n" +
        "    AND comment = 'Amazon Review Category Data'\n" +
        "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}\n" +
        "    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'};";
  }
}
