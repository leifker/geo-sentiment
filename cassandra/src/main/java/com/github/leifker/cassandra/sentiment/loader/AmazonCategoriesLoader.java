package com.github.leifker.cassandra.sentiment.loader;

import com.github.leifker.cassandra.config.AmazonReviewLoaderBeans;
import com.github.leifker.cassandra.sentiment.AmazonCategoryDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.*;

/**
 * Created by dleifker on 2/15/17.
 */
public class AmazonCategoriesLoader {
  private static final Logger LOG = LoggerFactory.getLogger(AmazonCategoriesLoader.class);

  public static void main(String [] args) {
    if (args.length == 0 || args[0] == null) {
      LOG.error("Amazon Review categories file must be provided as an argument.");
      System.exit(1);
    } else {
      LOG.info("Loading data from " + args[0]);
    }

    ApplicationContext ctx = new AnnotationConfigApplicationContext(AmazonReviewLoaderBeans.class);
    AmazonCategoryDao dao = ctx.getBean(AmazonCategoryDao.class);

    try (BufferedReader br = AmazonReviewLoader.fileReader(args[0])) {
      AmazonCategoriesParser.parse(br, dao);
    } catch (Exception e) {
      LOG.error("Error loading Amazon Data.", e);
      System.exit(1);
    }

    System.exit(0);
  }
}
