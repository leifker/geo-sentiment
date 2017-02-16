package com.github.leifker.cassandra.sentiment;

import com.github.leifker.cassandra.AbstractDao;
import com.github.leifker.cassandra.CassandraModel;
import com.github.leifker.cassandra.config.AmazonReviewsBeans;
import com.github.leifker.cassandra.config.CassandraTestBeans;
import com.github.mirreck.FakeFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

/**
 * Created by dleifker on 2/15/17.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class, classes = { AmazonReviewsBeans.class, CassandraTestBeans.class })
public abstract class AbstractDaoTest<PK, M extends CassandraModel<PK>, D extends AbstractDao<PK, M>> {
  protected final static FakeFactory FAKER = new FakeFactory();
  private final static int NUMBER_OF_MODELS = 100;

  @Before
  public void before() {
    getDao().truncate();
  }

  @Test
  public void testPersist() throws Exception {
    // given some models
    Set<M> expectedModels = IntStream.range(0, NUMBER_OF_MODELS).boxed().map(i -> getRandom()).collect(Collectors.toSet());
    // when persisted
    getDao().persist(expectedModels);
    // then readable
    Set<PK> keys = expectedModels.stream().map(CassandraModel::getPartitionKey).collect(Collectors.toSet());
    Set<M> actualReviews = getDao().streamByKey(keys).collect(Collectors.toSet());
    assertThat(actualReviews, containsInAnyOrder(expectedModels.toArray()));
  }

  @Test
  public void testDelete() throws Exception {
    // given some persisted reviews
    Set<M> expectedModels = IntStream.range(0, NUMBER_OF_MODELS).boxed().map(i -> getRandom()).collect(Collectors.toSet());
    getDao().persist(expectedModels);
    Set<PK> keys = expectedModels.stream().map(CassandraModel::getPartitionKey).collect(Collectors.toSet());
    Set<M> actualReviews = getDao().streamByKey(keys).collect(Collectors.toSet());
    assertThat(actualReviews, containsInAnyOrder(expectedModels.toArray()));
    // when deleted
    getDao().deleteByKey(keys);
    // then no results
    actualReviews = getDao().streamByKey(keys).collect(Collectors.toSet());
    assertThat(actualReviews, empty());
  }

  abstract D getDao();
  abstract M getRandom();
}
