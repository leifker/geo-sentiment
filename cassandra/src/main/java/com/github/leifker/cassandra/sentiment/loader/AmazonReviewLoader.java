package com.github.leifker.cassandra.sentiment.loader;

import com.github.leifker.cassandra.config.AmazonReviewLoaderBeans;
import com.github.leifker.cassandra.sentiment.AmazonCategoryDao;
import com.github.leifker.cassandra.sentiment.AmazonReviewDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by dleifker on 2/15/17.
 */
@Configuration
public class AmazonReviewLoader {
  private static final Logger LOG = LoggerFactory.getLogger(AmazonReviewLoader.class);

  public static void main(String [] args) {
    if (args.length == 0 || args[0] == null) {
      LOG.error("Amazon Review data file must be provided as an argument.");
      System.exit(1);
    } else {
      LOG.info("Loading data from " + args[0]);
    }

    ApplicationContext ctx = new AnnotationConfigApplicationContext(AmazonReviewLoaderBeans.class);
    AmazonReviewDao reviewDao = ctx.getBean(AmazonReviewDao.class);
    AmazonCategoryDao categoryDao = ctx.getBean(AmazonCategoryDao.class);

    try (BufferedReader br = fileReader(args[0])) {
      AmazonReviewParser.parse(br, categoryDao, reviewDao);
    } catch (Exception e) {
      LOG.error("Error loading Amazon data", e);
      System.exit(1);
    }

    System.exit(0);
  }

  public static BufferedReader fileReader(String filename) throws Exception {
    if (filename.endsWith(".gz")) {
      InputStream fileStream = new FileInputStream(filename);
      InputStream gzipStream = new GZIPInputStream(fileStream);
      Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
      return new BufferedReader(decoder);
    } else {
      return new BufferedReader(new FileReader(filename));
    }
  }
}
