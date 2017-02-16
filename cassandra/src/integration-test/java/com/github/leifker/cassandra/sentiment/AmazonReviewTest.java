package com.github.leifker.cassandra.sentiment;

import com.github.mirreck.FakeFactory;
import com.github.mirreck.RandomUtils;
import org.apache.commons.lang3.tuple.Triple;

import javax.inject.Inject;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Created by dleifker on 2/14/17.
 */
public class AmazonReviewTest extends AbstractDaoTest<Triple<String, Integer, String>, AmazonReview, AmazonReviewDao> {
  private final static FakeFactory FAKER = new FakeFactory();

  @Inject
  private AmazonReviewDao dao;

  @Override
  public AmazonReviewDao getDao() {
    return dao;
  }

  @Override
  public AmazonReview getRandom() {
    AmazonReview model = new AmazonReview();
    model.setPartitionKey(Triple.of(FAKER.letters(10), RandomUtils.intInInterval(0, 5), FAKER.bothify("####-????-#####")));
    model.setHelpfulness(String.format("%s/10", RandomUtils.intInInterval(0, 10)));
    model.setUserId(FAKER.numerify("###########"));
    model.setProfileName(FAKER.firstName());
    model.setTitle(String.join(" ", FAKER.words(RandomUtils.intInInterval(1, 5))));
    model.setSummary(FAKER.sentence());
    model.setReviewText(String.join(" ", FAKER.paragraphs(RandomUtils.intInInterval(1, 3))));
    model.setTime(
        (long) RandomUtils.intInInterval(
            (int) Instant.now().minus(90, ChronoUnit.DAYS).getEpochSecond(),
            (int) Instant.now().getEpochSecond()));
    return model;
  }
}