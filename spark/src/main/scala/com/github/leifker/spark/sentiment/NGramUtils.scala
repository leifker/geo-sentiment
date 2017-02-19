package com.github.leifker.spark.sentiment

import java.util.regex.Pattern

import scala.collection.mutable.{Buffer => MBuffer, Set => MSet}

/**
  * Created by dleifker on 2/18/17.
  */
object NGramUtils {
  /**
    * Considers social terms plus uni-grams, bi-grams, tri-grams (the grams are not subsets of the larger grams)
    * Preserves ordering of the grams and their frequency
    * @param text to anaylze
    * @return grams
    */
  def nGrams(text: String): Seq[String] = {
    NLPUtils.enchancedTokens(text) match {
      case Some(tokens) => {
        NLPUtils.socialTerms(tokens) ++ termNgrams(tokens, 1, 3)
      }
      case None => Seq.empty
    }
  }

  private def termNgrams(tokens: Seq[String], min: Int, max: Int): Seq[String] = {
    def partialSeqSet(seq: Vector[String], min: Int, max: Int): Seq[String] = {
      val termNgrams = MBuffer.empty[Vector[String]];
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

    NLPUtils.sentences(tokens, false).flatMap(seq => partialSeqSet(seq, min, max))
  }

  private def noIndexSlice(termFilters: MSet[Vector[String]], window: Vector[String]): Boolean = {
    termFilters.forall(existing => existing.indexOfSlice(window) == -1)
  }

  private val punctuationPattern = Pattern.compile("""["“”‘’'.?!…,:;»«()&]+""")
}
