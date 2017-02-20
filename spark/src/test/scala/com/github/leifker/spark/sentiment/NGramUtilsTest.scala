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

  it should "respect '.!?,' punctuation sentence boundaries" taggedAs(UnitTest) in {
    assert(NGramUtils.nGrams("This is the worst food ever. Best view of the venue though.").toSet == Set("worst", "best"))
    assert(NGramUtils.nGrams("This is the worst food ever! Best view of the venue though.").toSet == Set("worst!", "best"))
    assert(NGramUtils.nGrams("This is the worst food ever? Best view of the venue though.").toSet == Set("worst?", "best"))
    assert(NGramUtils.nGrams("This is the worst food ever, but the best view of the venue though.").toSet == Set("worst", "best"))
  }

  it should "respect 'and' sentence boundaries" taggedAs(UnitTest) in {
    assert(NGramUtils.nGrams("This is the worst food ever and the best view of the venue though.").toSet == Set("worst", "best"))
  }

  it should "preserve emoticons" taggedAs(UnitTest) in {
    assert(NGramUtils.nGrams("This is the worst food ever :-(").toSet.contains(":-("))
    assert(NGramUtils.nGrams("This is the worst food ever :-)").toSet.contains(":-)"))
    assert(NGramUtils.nGrams("This is the worst food ever ;-)").toSet.contains(";-)"))
    assert(NGramUtils.nGrams("This is the worst food ever :-))").toSet.contains(":-))"))
  }

  it should "preserve hashtags as individual terms" taggedAs(UnitTest) in {
    val terms = NGramUtils.nGrams("This is the worst food ever #winning #LOL").toSet
    assert(terms.contains("#winning"))
    assert(terms.contains("#lol"))
  }

  it should "handle this example A" taggedAs(UnitTest) in {
    assert(NGramUtils.nGrams("I can not give this item a 5 star rating, because it has its faults. First, it is not easy to install onto the gun. " +
      "You have to take the entire gun apart. When installing the internal parts, it comes with cheap, small o-rings which fray and break easily. " +
      "It also will not work if it is not always oiled and perfectly sealed.If you can get it onto the gun with no problems, " +
      "it can save your butt in a match.IT is a pretty good addition, especially because you can use it in an official paintball field without it counting as an automatic gun.")
    == Vector(
      "not give",
      "not easy install",
      "entire",
      "installing internal", "cheap", "small", "easily",
      "not work not", "work not oiled", "perfectly sealed",
      "save",
      "pretty good", "especially official counting", "official counting automatic"))
  }
}
