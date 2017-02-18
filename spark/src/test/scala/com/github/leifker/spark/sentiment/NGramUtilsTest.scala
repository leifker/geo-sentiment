package com.github.leifker.spark.sentiment

import com.github.leifker.spark.sentiment.test.UnitTest
import org.scalatest.FlatSpec

/**
  * Created by dleifker on 2/18/17.
  */
class NGramUtilsTest extends FlatSpec {
  "NGramUtils" should "be able to extract uni-grams without stop words" taggedAs(UnitTest) in {
    assert(NGramUtils.termNgrams("This is not good food", 3, 3) == Set("not good food"))
  }
}
