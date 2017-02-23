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

  "enhancedTokens" should "empty begets empty" taggedAs(UnitTest) in {
    assert(NLPUtils.enhancedTokens("") == Option.empty)
  }

  it should "non-english empty" taggedAs(UnitTest) in {
    assert(NLPUtils.enhancedTokens("Yo no hablo inglés.") == Option.empty)
  }

  it should "mark caps with exclaim" taggedAs(UnitTest) in {
    assert(NLPUtils.enhancedTokens("This is an INTERESTING test.") ==
      Option(Vector("interesting!", ".")))
  }

  it should "mark sentences with ? and ! when punctuation found as well as all caps" taggedAs(UnitTest) in {
    assert(NLPUtils.enhancedTokens("This is an INTERESTING test ! What the LOVELY test is this?") ==
      Option(Vector(
        "interesting!!", "!",
        "lovely!?", "?"
      )))
  }

  it should "mark sentences with ?! or !? when found as well as all caps" taggedAs(UnitTest) in {
    assert(NLPUtils.enhancedTokens("This is an INTERESTING test ! What LOVELY test is this? Is this test AWESOME!?!?") ==
      Option(Vector(
        "interesting!!", "!",
        "lovely!?", "?",
        "awesome!?!", "?!"
      )))
    assert(NLPUtils.enhancedTokens("This is an INTERESTING test ! What LOVELY test is this? Is this test AWESOME?!?!") ==
      Option(Vector(
        "interesting!!", "!",
        "lovely!?", "?",
        "awesome!?!", "?!"
      )))
  }

  it should "remove duplicate repeated characters" taggedAs(UnitTest) in {
    assert(NLPUtils.enhancedTokens("I looovvve this test") == Option(Vector("loovve")))
  }

  it should "patch when repeat punctuation" taggedAs(UnitTest) in {
    assert(
      NLPUtils.enhancedTokens("I looooooovvvvvvvve this test!!!!!!!!!!") ==
        Option(Vector("loovve!", "!")))
  }

  it should "should handle missing spaces" taggedAs(UnitTest) in {
    assert(
      NLPUtils.enhancedTokens("This is the most interesting test I've every taken.It is clearly the best test ever.") ==
        Option(Vector("interesting", ".", "best", ".")))
  }

  "stanfordFilter" should "strip nouns from sentences" taggedAs(UnitTest) in {
    assert(NLPUtils.posFilter(Vector("this", "is", "a", "lovely", "test", ".", "this", "is", "not", "a", "red", "rubber", "ball", "!")) ==
      Vector("is", "lovely", ".", "is", "not", "red", "!"))
  }
}
