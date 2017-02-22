package com.github.leifker.cassandra.sentiment;

import com.github.leifker.cassandra.config.AmazonReviewsBeans;
import com.github.leifker.cassandra.config.CassandraTestBeans;
import com.github.leifker.cassandra.sentiment.loader.AmazonCategoriesParser;
import com.github.leifker.cassandra.sentiment.loader.AmazonReviewParser;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Created by dleifker on 2/16/17.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class, classes = { AmazonReviewsBeans.class, CassandraTestBeans.class })
public class LoaderFixtureTest {
  @Inject
  private AmazonCategoryDao categoryDao;
  @Inject
  private AmazonReviewDao reviewDao;

  @Before
  public void before() {
    categoryDao.truncate();
    reviewDao.truncate();
  }

  private static final Set<String> expectedProductIds = IntStream.range(0, 13).boxed()
      .map(i -> String.format("%010d", i))
      .collect(Collectors.toSet());
  private static final Map<String, String> expectedRootCategories = new ImmutableMap.Builder<String, String>()
      .put("0000000001", "Books")
      .put("0000000002", "Music")
      .put("0000000003", "Tools & Home Improvement")
      .put("0000000004", "Music")
      .put("0000000005", "Sports & Outdoors")
      .put("0000000006", "Books")
      .put("0000000007", "Sports & Outdoors")
      .put("0000000008", "Books")
      .put("0000000009", "Music")
      .put("0000000010", "")
      .put("0000000011", "Movies & TV")
      .put("0000000012", "Office Products")
      .build();
  private static final Map<String, Set<String>> expectedCategoryPaths = new ImmutableMap.Builder<String, Set<String>>()
      .put("0000000001", ImmutableSet.of("Books, Education & Reference, Foreign Language Study & Reference",
                                         "Books, Literature & Fiction, Contemporary",
                                         "Books, Literature & Fiction, Foreign Language Fiction",
                                         "Books, Literature & Fiction, World Literature",
                                         "Books, Science Fiction & Fantasy, Science Fiction"))
      .put("0000000002", ImmutableSet.of("Music, Blues", "Music, Pop", "Music, R&B"))
      .put("0000000003", ImmutableSet.of("Tools & Home Improvement, Lighting & Ceiling Fans, Lamps & Shades, Floor Lamps"))
      .put("0000000004", ImmutableSet.of("Music, Classic Rock", "Music, Pop", "Music, R&B", "Music, Soundtracks"))
      .put("0000000005", ImmutableSet.of("Sports & Outdoors"))
      .put("0000000006", ImmutableSet.of("Books, Law, Criminal Law, Forensic Science", "Books, New, Used & Rental Textbooks"))
      .put("0000000007", ImmutableSet.of("Sports & Outdoors, Hunting & Fishing, Fishing, Lures & Flies"))
      .put("0000000008", ImmutableSet.of("Books"))
      .put("0000000009", ImmutableSet.of("Music, Jazz", "Music, Pop"))
      .put("0000000010", ImmutableSet.of(""))
      .put("0000000011", ImmutableSet.of("Movies & TV, Movies"))
      .put("0000000012", ImmutableSet.of("Office Products, Office & School Supplies, Cutting & Measuring Devices, Rulers & Tape Measures"))
      .build();

  @Test
  public void categoryLoaderTest() throws Exception {
    // when: categories loaded
    try (BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/fixtures/categories.txt")))) {
      AmazonCategoriesParser.parse(br, categoryDao);
    }

    // then: root categories loaded
    assertThat(categoryDao.findRootCategories(expectedProductIds).entrySet(), containsInAnyOrder(expectedRootCategories.entrySet().toArray()));
    // then: category paths loaded
    assertThat(categoryDao.streamByKey(expectedProductIds).collect(
        Collectors.groupingBy(AmazonCategory::getProductId, Collectors.mapping(AmazonCategory::getCategoryPath, Collectors.toSet()))).entrySet(),
        containsInAnyOrder(expectedCategoryPaths.entrySet().toArray()));
  }

  private static final Set<Pair<String, Integer>> expectedReviewKeys = ImmutableSet.of(
      Pair.of("Books", 5),
      Pair.of("Music", 4),
      Pair.of("Music", 5),
      Pair.of("Tools & Home Improvement", 2),
      Pair.of("Tools & Home Improvement", 4),
      Pair.of("Music", 4),
      Pair.of("Music", 5),
      Pair.of("Sports & Outdoors", 5),
      Pair.of("Books", 2),
      Pair.of("Books", 5),
      Pair.of("Sports & Outdoors", 2),
      Pair.of("Sports & Outdoors", 5),
      Pair.of("Books", 1),
      Pair.of("Music", 5),
      Pair.of("", 5),
      Pair.of("Movies & TV", 5),
      Pair.of("Office Products", 1)
  );
  private static final Map<Pair<String, Integer>, Set<String>> expectedScoredReviews = new ImmutableMap.Builder<Pair<String, Integer>, Set<String>>()
      .put(Pair.of("0000000001", 5), ImmutableSet.of("review 1", "review 13"))
      .put(Pair.of("0000000002", 4), ImmutableSet.of("review 14"))
      .put(Pair.of("0000000002", 5), ImmutableSet.of("review 2"))
      .put(Pair.of("0000000003", 2), ImmutableSet.of("review 3"))
      .put(Pair.of("0000000003", 4), ImmutableSet.of("review 15"))
      .put(Pair.of("0000000004", 4), ImmutableSet.of("review 16"))
      .put(Pair.of("0000000004", 5), ImmutableSet.of("review 4"))
      .put(Pair.of("0000000005", 5), ImmutableSet.of("review 5", "review 17"))
      .put(Pair.of("0000000006", 2), ImmutableSet.of("review 6"))
      .put(Pair.of("0000000006", 5), ImmutableSet.of("review 18"))
      .put(Pair.of("0000000007", 5), ImmutableSet.of("review 7"))
      .put(Pair.of("0000000008", 1), ImmutableSet.of("review 8"))
      .put(Pair.of("0000000009", 5), ImmutableSet.of("review 9"))
      .put(Pair.of("0000000010", 5), ImmutableSet.of("review 10"))
      .put(Pair.of("0000000011", 5), ImmutableSet.of("review 11"))
      .put(Pair.of("0000000012", 1), ImmutableSet.of("review 12"))
      .build();

  @Test
  public void reviewLoaderTest() throws Exception {
    // when: categories loaded
    try (BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/fixtures/categories.txt")))) {
      AmazonCategoriesParser.parse(br, categoryDao);
    }
    // when: reviews loaded
    try (BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/fixtures/reviews.txt")))) {
      AmazonReviewParser.parse(br, categoryDao, reviewDao);
    }

    // then: reviews loaded
    assertThat(reviewDao.streamByKey(expectedReviewKeys).collect(Collectors
        .groupingBy(r -> Pair.of(r.getProductId(), r.getScore()), Collectors.mapping(AmazonReview::getReviewText, Collectors.toSet()))).entrySet(),
        containsInAnyOrder(expectedScoredReviews.entrySet().toArray()));
  }
}
