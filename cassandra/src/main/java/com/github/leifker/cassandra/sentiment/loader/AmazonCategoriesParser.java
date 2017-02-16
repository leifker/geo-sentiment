package com.github.leifker.cassandra.sentiment.loader;

import com.github.leifker.cassandra.sentiment.AmazonCategory;
import com.github.leifker.cassandra.sentiment.AmazonCategoryDao;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dleifker on 2/15/17.
 */
public class AmazonCategoriesParser {
  private static final Logger LOG = LoggerFactory.getLogger(AmazonCategoriesParser.class);
  private static String SEPARATOR = ", ";
  private static int CATEGORY_PREFIX_CHARS = 2;

  public static void parse(BufferedReader br, AmazonCategoryDao dao) {
    Preconditions.checkNotNull(dao);
    Preconditions.checkNotNull(br);

    String line = null;
    String productId = null;
    long modelCount = 0L;

    try {
      List<AmazonCategory> batch = new ArrayList<>();

      while ((line = br.readLine()) != null) {
        if ((line.startsWith("  ") || "".equals(line)) && productId != null) {
          batch.add(buildModel(productId, line));
        } else {
          productId = line;
          dao.persist(batch);
          modelCount += batch.size();
          LOG.info(String.format("Wrote %s models.", modelCount));
          batch.clear();
        }
      }

      // flush any pending writes
      dao.persist(batch);
      modelCount += batch.size();
      LOG.info(String.format("Wrote %s models.", modelCount));

    } catch (Exception e) {
      LOG.error(String.format("Error loading Amazon Data. ProductId:%s Line:%s", productId, line), e);
      System.exit(1);
    }
  }

  private static AmazonCategory buildModel(String productId, String line) {
    AmazonCategory model = new AmazonCategory();
    model.setProductId(productId);

    if (line.length() > CATEGORY_PREFIX_CHARS) {
      model.setCategoryPath(line.substring(CATEGORY_PREFIX_CHARS));

      String[] categories = StringUtils.splitByWholeSeparator(model.getCategoryPath(), SEPARATOR, 2);
      if (categories.length > 0) {
        model.setRootCategory(categories[0]);
      }
    }

    return model;
  }
}
