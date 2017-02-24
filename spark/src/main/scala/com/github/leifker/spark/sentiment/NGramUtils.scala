package com.github.leifker.spark.sentiment

import java.util.regex.Pattern

import scala.collection.mutable.{Buffer => MBuffer, Set => MSet}

/**
  * Created by dleifker on 2/18/17.
  */
object NGramUtils {
  private val minTokenSize = 2;
  /**
    * Considers social terms plus uni-grams, bi-grams, tri-grams (the grams are not subsets of the larger grams)
    * Preserves ordering of the grams and their frequency
    * @param text to anaylze
    * @return grams
    */
  def nGrams(text: String, max: Int = 3): Seq[String] = {
    val tokens = NLPUtils.tokenize(text)
    val hashTagTokens = NLPUtils.hashTagTokens(tokens)
    val emoijTokens = NLPUtils.emojiTokens(tokens)
    val enhancedTokens = if (NLPUtils.isEnglish(text)) termNgrams(NLPUtils.enhancedTokens(tokens), 1, max) else Seq.empty

    enhancedTokens ++ hashTagTokens ++ emoijTokens
  }

  private def termNgrams(tokens: Seq[String], min: Int, max: Int): Seq[String] = {
    def partialSeqSet(seq: Vector[String], min: Int, max: Int): Seq[String] = {
      val termNgrams = MBuffer.empty[Vector[String]]
      val termFilters = MSet.empty[Vector[String]]
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

    NLPUtils.sentences(tokens, false)
      .map(sentence => sentence.filter(ngramTokenFilter))
      .flatMap(seq => partialSeqSet(seq, min, max))
  }

  private def noIndexSlice(termFilters: MSet[Vector[String]], window: Vector[String]): Boolean = {
    termFilters.forall(existing => existing.indexOfSlice(window) == -1)
  }

  val htmlEntity = Pattern.compile("""^&\w+;$""")
  private def ngramTokenFilter(word: String): Boolean = {
    word.length >= minTokenSize &&
    !NLPUtils.allPunctuation.matcher(word).matches() &&
    !htmlEntity.matcher(word).matches()
  }
}
