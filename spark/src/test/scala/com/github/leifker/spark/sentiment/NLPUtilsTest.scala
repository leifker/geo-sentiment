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
    assert(NLPUtils.enchancedTokens("") == Option.empty)
  }

  it should "non-english empty" taggedAs(UnitTest) in {
    assert(NLPUtils.enchancedTokens("Yo no hablo inglés.") == Option.empty)
  }

  it should "mark caps with exclaim" taggedAs(UnitTest) in {
    assert(NLPUtils.enchancedTokens("This is an INTERESTING test.") ==
      Option(Vector("interesting!", ".")))
  }

  it should "mark sentences with ? and ! when punctuation found as well as all caps" taggedAs(UnitTest) in {
    assert(NLPUtils.enchancedTokens("This is an INTERESTING test ! What the LOVELY test is this?") ==
      Option(Vector(
        "interesting!!", "!",
        "lovely!?", "?"
      )))
  }

  it should "mark sentences with ?! or !? when found as well as all caps" taggedAs(UnitTest) in {
    assert(NLPUtils.enchancedTokens("This is an INTERESTING test ! What LOVELY test is this? Is this test AWESOME!?!?") ==
      Option(Vector(
        "interesting!!", "!",
        "lovely!?", "?",
        "awesome!?!", "?!"
      )))
    assert(NLPUtils.enchancedTokens("This is an INTERESTING test ! What LOVELY test is this? Is this test AWESOME?!?!") ==
      Option(Vector(
        "interesting!!", "!",
        "lovely!?", "?",
        "awesome!?!", "?!"
      )))
  }

  it should "remove duplicate repeated characters" taggedAs(UnitTest) in {
    assert(NLPUtils.enchancedTokens("I looovvve this test") == Option(Vector("loovve")))
  }

  it should "patch when repeat punctuation" taggedAs(UnitTest) in {
    assert(
      NLPUtils.enchancedTokens("I looooooovvvvvvvve this test!!!!!!!!!!") ==
        Option(Vector("loovve!", "!")))
  }

  "patchTokens" should "pass thru vectors without specified punctuation" taggedAs(UnitTest) in {
    val testVector = Vector("this", "is", "a", "sentence", "without", "punctuation", "of", "interest", ".")
    assert(NLPUtils.patchToken(testVector, "!", s => s + "!") == testVector)
  }

  it should "pass thru junk punctuation" taggedAs(UnitTest) in {
    val testVector = Vector("?","!",".","!","!")
    assert(NLPUtils.patchToken(testVector, "!", s => s + "!") == testVector)
  }

  it should "patch when there is no previous punctuation" taggedAs(UnitTest) in {
    val testVector = Vector("This", "is", "a", "sentence", "of", "interest", "!")
    assert(
      NLPUtils.patchToken(testVector, "!", s => s + "!") ==
        Vector("This!", "is!", "a!", "sentence!", "of!", "interest!", "!"))
  }

  it should "patch when repeat punctuation" taggedAs(UnitTest) in {
    val testVector = Vector("This", "is", "a", "sentence", "of", "interest", "!", "!", "!")
    assert(
      NLPUtils.patchToken(testVector, "!", s => s + "!") ==
        Vector("This!", "is!", "a!", "sentence!", "of!", "interest!", "!", "!", "!"))
  }

  it should "patch when duplicate punctuation no previous punctuation" taggedAs(UnitTest) in {
    val testVector = Vector("This", "is", "a", "sentence", "of", "interest", "!")
    assert(
      NLPUtils.patchToken(testVector, "!", s => s + "!") ==
        Vector("This!", "is!", "a!", "sentence!", "of!", "interest!", "!"))
  }

  it should "patch only the target sentence when previous sentence is present" taggedAs(UnitTest) in {
    val testVector = Vector(
      "This", "is", "not", "patched", ".",
      "This", "is", "a", "sentence", "of", "interest", "!")
    assert(
      NLPUtils.patchToken(testVector, "!", s => s + "!") ==
        Vector(
          "This", "is", "not", "patched", ".",
          "This!", "is!", "a!", "sentence!", "of!", "interest!", "!"))
  }

  it should "patch only the target sentence when following sentence is present" taggedAs(UnitTest) in {
    val testVector = Vector(
      "This", "is", "a", "sentence", "of", "interest", "!",
      "This", "is", "not", "patched", ".")
    assert(
      NLPUtils.patchToken(testVector, "!", s => s + "!") ==
        Vector(
          "This!", "is!", "a!", "sentence!", "of!", "interest!", "!",
          "This", "is", "not", "patched", "."))
  }

  it should "patch only the target sentence when before and after sentence is present" taggedAs(UnitTest) in {
    val testVector = Vector(
      "This", "is", "not", "patched", ".",
      "This", "is", "a", "sentence", "of", "interest", "!",
      "This", "is", "not", "patched", ".")
    assert(
      NLPUtils.patchToken(testVector, "!", s => s + "!") ==
        Vector(
          "This", "is", "not", "patched", ".",
          "This!", "is!", "a!", "sentence!", "of!", "interest!", "!",
          "This", "is", "not", "patched", "."))
  }

  it should "patch multiple target sentences with interspersed non-targets" taggedAs(UnitTest) in {
    val testVector = Vector(
      "This", "is", "not", "patched", ".",
      "This", "is", "a", "sentence", "of", "interest", "!",
      "This", "is", "not", "patched", ".",
      "This", "is", "a", "sentence", "of", "interest", "!")
    assert(
      NLPUtils.patchToken(testVector, "!", s => s + "!") ==
        Vector("This", "is", "not", "patched", ".",
          "This!", "is!", "a!", "sentence!", "of!", "interest!", "!",
          "This", "is", "not", "patched", ".",
          "This!", "is!", "a!", "sentence!", "of!", "interest!", "!"))
  }

  it should "patch multiple target sentences" taggedAs(UnitTest) in {
    val testVector = Vector(
      "This", "is", "a", "sentence", "of", "interest", "!",
      "This", "is", "a", "sentence", "of", "interest", "!")
    assert(
      NLPUtils.patchToken(testVector, "!", s => s + "!") ==
        Vector(
          "This!", "is!", "a!", "sentence!", "of!", "interest!", "!",
          "This!", "is!", "a!", "sentence!", "of!", "interest!", "!"))
  }

  "stanfordFilter" should "strip nouns from sentences" taggedAs(UnitTest) in {
    assert(NLPUtils.posFilter(Vector("this", "is", "a", "lovely", "test", ".", "this", "is", "not", "a", "red", "rubber", "ball", "!")) ==
      Vector("is", "lovely", ".", "is", "not", "red", "!"))
  }
}
