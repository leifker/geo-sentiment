package com.github.leifker.cassandra.sentiment;

import javax.inject.Inject;
import java.util.List;

/**
 * Created by dleifker on 2/14/17.
 */
public class AmazonCategoryTest extends AbstractDaoTest<String, AmazonCategory, AmazonCategoryDao> {
  @Inject
  private AmazonCategoryDao dao;

  @Override
  AmazonCategoryDao getDao() {
    return dao;
  }

  @Override
  public AmazonCategory getRandom() {
    AmazonCategory model = new AmazonCategory();
    model.setPartitionKey(FAKER.bothify("####-????-#####"));
    List<String> categories = FAKER.words(6);
    model.setRootCategory(categories.get(0));
    model.setCategoryPath(String.join(", ", categories));
    return model;
  }
}