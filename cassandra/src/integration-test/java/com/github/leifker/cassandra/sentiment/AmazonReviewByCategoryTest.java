package com.github.leifker.cassandra.sentiment;

import com.github.leifker.cassandra.config.AmazonReviewsBeans;
import com.github.leifker.cassandra.config.CassandraTestBeans;
import com.github.mirreck.FakeFactory;
import com.github.mirreck.RandomUtils;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import javax.inject.Inject;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyCollectionOf;

/**
 * Created by dleifker on 2/14/17.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class, classes = { AmazonReviewsBeans.class, CassandraTestBeans.class })
public class AmazonReviewByCategoryTest {
  private final static FakeFactory FAKER = new FakeFactory();

  @Inject
  private AmazonReviewByCategoryDao dao;

  @Before
  public void before() {
    dao.truncate();
  }

  @Test
  public void testPersist() throws Exception {
    // given some reviews
    Set<AmazonReviewByCategory> expectedReviews = ImmutableSet.of(randomReview(), randomReview(), randomReview(), randomReview());
    // when persisted
    dao.persist(expectedReviews);
    // then readable
    Set<AmazonReviewByCategory> actualReviews = dao.streamByKey(expectedReviews.stream().map(AmazonReviewByCategory::getPartitionKey).collect(Collectors.toSet()))
        .collect(Collectors.toSet());
    assertThat(actualReviews, containsInAnyOrder(expectedReviews.toArray()));
  }

  @Test
  public void testDelete() throws Exception {
    // given some persisted reviews
    Set<AmazonReviewByCategory> expectedReviews = ImmutableSet.of(randomReview(), randomReview(), randomReview(), randomReview());
    dao.persist(expectedReviews);
    Set<AmazonReviewByCategory> actualReviews = dao.streamByKey(expectedReviews.stream().map(AmazonReviewByCategory::getPartitionKey).collect(Collectors.toSet()))
        .collect(Collectors.toSet());
    assertThat(actualReviews, containsInAnyOrder(expectedReviews.toArray()));
    // when deleted
    dao.deleteByKey(expectedReviews.stream().map(AmazonReviewByCategory::getPartitionKey).collect(Collectors.toSet()));
    // then no results
    actualReviews = dao.streamByKey(expectedReviews.stream().map(AmazonReviewByCategory::getPartitionKey).collect(Collectors.toSet()))
        .collect(Collectors.toSet());
    assertThat(actualReviews, emptyCollectionOf(AmazonReviewByCategory.class));
  }

  private static AmazonReviewByCategory randomReview() {
    AmazonReviewByCategory model = new AmazonReviewByCategory();
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