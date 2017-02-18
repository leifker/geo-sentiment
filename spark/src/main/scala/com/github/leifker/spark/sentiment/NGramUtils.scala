package com.github.leifker.spark.sentiment

import com.peoplepattern.text.LangBundle

import scala.collection.mutable.{ Buffer, Set => MSet }

/**
  * Created by dleifker on 2/18/17.
  */
object NGramUtils {
  lazy val enBundle: LangBundle = LangBundle.bundleForLang(Option("en"))

  /**
    * Extract the set of term-only n-grams from the text
    *
    * For example from the text "this is the winning team" only the bigram
    * "winning team" would be extracted
    *
    * @param text the text to extract n-grams from
    * @param min the minimum length of extracted n-grams
    * @param max the maximum length of extracted n-grams
    */
  def termNgrams(text: String, min: Int, max: Int): Set[String] = {
    NLPUtils.enchancedTokens(text) match {
      case Some(tokens) => termNgrams(tokens, min, max)
      case None => Set.empty
    }
  }

  def termNgrams(tokens: Seq[String], min: Int, max: Int): Set[String] = {
    val seqs = Buffer.empty[Vector[String]]
    val thisbf = Buffer.empty[String]
    for (token <- tokens) {
      if (enBundle.isContentTerm(token)) {
        thisbf += token.toLowerCase
      } else {
        if (thisbf.nonEmpty) {
          seqs += thisbf.toVector
        }
        thisbf.clear()
      }
    }
    // flush buffer
    if (thisbf.nonEmpty) {
      seqs += thisbf.toVector
    }
    val termNgrams = MSet.empty[String]
    for {
      n <- min to max
      seq <- seqs if seq.size >= n
    } termNgrams ++= seq.sliding(n).map(_.mkString(" "))
    termNgrams.toSet
  }
}
