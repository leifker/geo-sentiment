package com.github.leifker.spark.sentiment

import scala.collection.mutable.{Buffer => MBuffer, Set => MSet}

/**
  * Created by dleifker on 2/18/17.
  */
object NGramUtils {
  def termNgrams(sentences: Seq[Vector[String]], min: Int = 1, max: Int = 3, shadeNgrams: Boolean = false): Seq[String] = {
    def partialSeqSet(seq: Vector[String], min: Int, max: Int): Seq[String] = {
      val termNgrams = MBuffer.empty[Vector[String]]
      val termFilters = MSet.empty[Vector[String]]
      for {
        n <- Math.min(max, seq.size) to min by -1
      } {
        termNgrams ++= seq.sliding(n).filter({
          case win if !shadeNgrams || termFilters.contains(win) => true
          case win if noIndexSlice(termFilters, win) =>
            termFilters += win
            true
          case _ => false
        })
      }
      termNgrams.map(_.mkString(" "))
    }

    sentences
      .map(sentence => sentence.filter(ngramTokenFilter))
      .flatMap(seq => partialSeqSet(seq, min, max))
  }

  private def noIndexSlice(termFilters: MSet[Vector[String]], window: Vector[String]): Boolean = {
    termFilters.forall(existing => existing.indexOfSlice(window) == -1)
  }

  private def ngramTokenFilter(word: String): Boolean = {
    !Constants.allPunctuation.matcher(word).matches() &&
    !Constants.htmlEntity.matcher(word).matches()
  }
}
