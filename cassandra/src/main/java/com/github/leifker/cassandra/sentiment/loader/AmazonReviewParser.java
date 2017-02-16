package com.github.leifker.cassandra.sentiment.loader;

import com.github.leifker.cassandra.AbstractDao;
import com.github.leifker.cassandra.CassandraModel;
import com.github.leifker.cassandra.sentiment.AmazonCategoryDao;
import com.github.leifker.cassandra.sentiment.AmazonReview;
import com.github.leifker.cassandra.sentiment.AmazonReviewDao;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dleifker on 2/15/17.
 */
@Configuration
public class AmazonReviewParser {
  private static final int batchSize = 10000;
  private static final int linesPerRecord = 11;
  private static final Logger LOG = LoggerFactory.getLogger(AmazonReviewParser.class);

  private final static ImmutableList<String> prefixes = ImmutableList.of(
      "product/productId: ",
      "product/title: ",
      "product/price: ",
      "review/userId: ",
      "review/profileName: ",
      "review/helpfulness: ",
      "review/score: ",
      "review/time: ",
      "review/summary: ",
      "review/text: "
  );

  public static void parse(BufferedReader br, AmazonCategoryDao categoryDao, AmazonReviewDao reviewDao) {
    AtomicLong modelCount = new AtomicLong();
    Iterators.partition(br.lines().iterator(), batchSize * linesPerRecord)
        .forEachRemaining(lines -> {
          List<AmazonReview> batch = new ArrayList<>();

          int idx = 0;
          AmazonReview model = null;
          for (String line : lines) {
            // parse lines into model
            switch(idx) {
              case 0:
                model = new AmazonReview();
                model.setProductId(stripPrefix(line, idx));
                break;
              case 1:
                model.setTitle(stripPrefix(line, idx));
                break;
              case 2:
                model.setPrice(stripPrefix(line, idx));
                break;
              case 3:
                model.setUserId(stripPrefix(line, idx));
                break;
              case 4:
                model.setProfileName(stripPrefix(line, idx));
                break;
              case 5:
                model.setHelpfulness(stripPrefix(line, idx));
                break;
              case 6:
                model.setScore(Double.valueOf(stripPrefix(line, idx)).intValue());
                break;
              case 7:
                model.setTime(Long.valueOf(stripPrefix(line, idx)));
                break;
              case 8:
                model.setSummary(stripPrefix(line, idx));
                break;
              case 9:
                model.setReviewText(stripPrefix(line, idx));
                batch.add(model);
                break;
              case 10:
                idx = -1;  // reset index
                break;
            }
            idx++;
          }

          // write batch of records
          try {
            reviewDao.persist(withCategory(categoryDao, batch));
            LOG.info(String.format("Wrote %s models.", modelCount.accumulateAndGet(batch.size(), Long::sum)));
            batch.clear();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  private static String stripPrefix(String line, int prefixIdx) throws RuntimeException {
    final String prefix = prefixes.get(prefixIdx);
    // sanity check
    if (!line.startsWith(prefix)) {
      throw new RuntimeException(String.format("Unexpected line in input file expected prefix:%s found:%s", prefix, line));
    }
    return line.substring(prefix.length());
  }

  private static List<AmazonReview> withCategory(AmazonCategoryDao dao, List<AmazonReview> reviews) {
    try {
      Map<String, List<AmazonReview>> reviewMap = reviews.stream()
          .collect(Collectors.groupingBy(AmazonReview::getProductId, Collectors.mapping(Function.identity(), Collectors.toList())));
      Map<String, String> categoryMap = dao.findRootCategories(reviewMap.keySet());

      return reviewMap.values().stream().flatMap(List::stream)
          .map(r -> {
            if (categoryMap.containsKey(r.getProductId())) {
              r.setRootCategory(categoryMap.get(r.getProductId()));
            } else {
              LOG.warn("Failed to determine root category for productId:" + r.getProductId());
            }
            return r;
          }).collect(Collectors.toList());

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
