package com.github.leifker.spark.sentiment

import java.util.regex.Pattern

import com.peoplepattern.text.LangBundle

import scala.collection.mutable.{Buffer, Seq => MSeq, Set => MSet}

/**
  * Created by dleifker on 2/18/17.
  */
object NGramUtils {
  lazy val enBundle: LangBundle = LangBundle.bundleForLang(Option("en"))

  /**
    * Considers social terms plus uni-grams, bi-grams, tri-grams (the grams are not subsets of the larger grams)
    * Preserves ordering of the grams and their frequency
    * @param text to anaylze
    * @return grams
    */
  def nGrams(text: String): Seq[String] = {
    NLPUtils.enchancedTokens(text) match {
      case Some(tokens) => {
        socialTerms(tokens) ++ termNgrams(tokens, 1, 3)
      }
      case None => Seq.empty
    }
  }

  private def socialTerms(tokens: Seq[String]): Seq[String] = {
    tokens.filter(w => enBundle.isMention(w) || enBundle.isHashtag(w)).map(_.toLowerCase).distinct
  }

  private def termNgrams(tokens: Seq[String], min: Int, max: Int): Seq[String] = {
    val seqs = Buffer.empty[Vector[String]]
    val thisbf = Buffer.empty[String]
    for (token <- tokens.filter(!enBundle.stopwords(_))) {
      if (!punctuationPattern.matcher(token).matches()) {
        thisbf += token
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

    seqs.flatMap(seq => partialSeqSet(seq, min, max))
  }

  private def partialSeqSet(seq: Vector[String], min: Int, max: Int): Seq[String] = {
    val termNgrams = Buffer.empty[Vector[String]];
    val termFilters = MSet.empty[Vector[String]];
    for {
      n <- Math.min(max, seq.size) to min by -1
    } {
      termNgrams ++= seq.sliding(n).filter({
        case win if termFilters.contains(win) => true
        case win if noIndexSlice(termFilters, win) => {
          termFilters += win
          true
        }
        case _ => false
      })
    }
    termNgrams.map(_.mkString(" "))
  }

  private def noIndexSlice(termFilters: MSet[Vector[String]], window: Vector[String]): Boolean = {
    termFilters.forall(existing => existing.indexOfSlice(window) == -1)
  }

  private val punctuationPattern = Pattern.compile("""["“”‘’'.?!…,:;»«()&]+""")
}
