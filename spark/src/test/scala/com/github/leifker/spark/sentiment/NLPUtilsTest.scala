package com.github.leifker.spark.sentiment

import com.github.leifker.spark.sentiment.test.UnitTest
import org.scalatest.FlatSpec

/**
  * Created by dleifker on 2/16/17.
  */
class NLPUtilsTest extends FlatSpec {

  "NLPUtils" should "be able to detect English" taggedAs(UnitTest) in {
    assert(NLPUtils.isEnglish("This should be english."))
  }

  it should "be able to detect non-English" taggedAs(UnitTest) in {
    assert(!NLPUtils.isEnglish("Yo no hablo inglés."))
  }

  "stanfordFilter" should "strip nouns from sentences" taggedAs(UnitTest) in {
    assert(NLPUtils.sentenceFilter(Seq(Vector("this", "is", "a", "lovely", "test", ".", "this", "is", "not", "a", "red", "rubber", "ball", "!"))) ==
      Seq(Vector("is", "lovely", ".", "is", "not", "red", "!")))
  }
}
