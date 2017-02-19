package com.github.leifker.spark.sentiment

import com.github.leifker.spark.sentiment.test.UnitTest
import org.scalatest.FlatSpec

/**
  * Created by dleifker on 2/18/17.
  */
class NGramUtilsTest extends FlatSpec {
  "NGramUtils" should "be able to extract n-grams without stop words" taggedAs(UnitTest) in {
    assert(NGramUtils.nGrams("This is not good food").toSet == Set("not good"))
  }

  it should "respect sentence boundaries" taggedAs(UnitTest) in {
    assert(NGramUtils.nGrams("This is the worst food ever. Best view of the venue though.").toSet == Set("worst", "best"))
  }
}
