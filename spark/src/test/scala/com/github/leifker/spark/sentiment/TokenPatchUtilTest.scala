package com.github.leifker.spark.sentiment

import com.github.leifker.spark.sentiment.test.UnitTest
import org.scalatest.FlatSpec

/**
  * Created by dleifker on 2/16/17.
  */
class TokenPatchUtilTest extends FlatSpec {
  val tokenPatcher = TokenPatchUtil()

  "TokenPatchUtil" should "pass thru vectors without specified punctuation" taggedAs(UnitTest) in {
    val testVector = Vector("this", "is", "a", "sentence", "without", "punctuation", "of", "interest", ".")
    assert(tokenPatcher.patchWithPunctuation(testVector) == testVector)
  }

  it should "pass thru junk punctuation" taggedAs(UnitTest) in {
    val testVector = Vector("?","!",".","!","!")
    assert(tokenPatcher.patchWithPunctuation(testVector) == testVector)
  }

  it should "patch when there is no previous punctuation" taggedAs(UnitTest) in {
    val testVector = Vector("This", "is", "a", "sentence", "of", "interest", "!")
    assert(
      tokenPatcher.patchWithPunctuation(testVector) ==
        Vector("This!", "is!", "a!", "sentence!", "of!", "interest!", "!"))
  }

  it should "patch when repeat punctuation" taggedAs(UnitTest) in {
    val testVector = Vector("This", "is", "a", "sentence", "of", "interest", "!", "!", "!")
    assert(
      tokenPatcher.patchWithPunctuation(testVector) ==
        Vector("This!", "is!", "a!", "sentence!", "of!", "interest!", "!", "!", "!"))
  }

  it should "patch when duplicate punctuation no previous punctuation" taggedAs(UnitTest) in {
    val testVector = Vector("This", "is", "a", "sentence", "of", "interest", "!")
    assert(
      tokenPatcher.patchWithPunctuation(testVector) ==
        Vector("This!", "is!", "a!", "sentence!", "of!", "interest!", "!"))
  }

  it should "patch only the target sentence when previous sentence is present" taggedAs(UnitTest) in {
    val testVector = Vector(
      "This", "is", "not", "patched", ".",
      "This", "is", "a", "sentence", "of", "interest", "!")
    assert(
      tokenPatcher.patchWithPunctuation(testVector) ==
        Vector(
          "This", "is", "not", "patched", ".",
          "This!", "is!", "a!", "sentence!", "of!", "interest!", "!"))
  }

  it should "patch only the target sentence when following sentence is present" taggedAs(UnitTest) in {
    val testVector = Vector(
      "This", "is", "a", "sentence", "of", "interest", "!",
      "This", "is", "not", "patched", ".")
    assert(
      tokenPatcher.patchWithPunctuation(testVector) ==
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
      tokenPatcher.patchWithPunctuation(testVector) ==
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
      tokenPatcher.patchWithPunctuation(testVector) ==
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
      tokenPatcher.patchWithPunctuation(testVector) ==
        Vector(
          "This!", "is!", "a!", "sentence!", "of!", "interest!", "!",
          "This!", "is!", "a!", "sentence!", "of!", "interest!", "!"))
  }
}
