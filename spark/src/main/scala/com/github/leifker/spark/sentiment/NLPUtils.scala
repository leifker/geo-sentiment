package com.github.leifker.spark.sentiment

import com.github.leifker.spark.sentiment.POSMode.POSMode
import com.peoplepattern.text.Implicits._
import com.peoplepattern.text.LangBundle
import edu.stanford.nlp.ling.{TaggedWord, Word}
import edu.stanford.nlp.tagger.maxent.MaxentTagger

import collection.JavaConverters._

/**
  * Created by dleifker on 2/16/17.
  */
object POSMode extends Enumeration {
  type POSMode = Value
  val None, IncludeNouns, FilterNouns = Value
}

object NLPUtils {
  lazy val enBundle: LangBundle = LangBundle.bundleForLang(Option("en"))
  lazy val tagger: MaxentTagger = new MaxentTagger("models/english-caseless-left3words-distsim.tagger")

  val isEnglish = (text: String) => text.lang.contains("en")
  val isHashtag = (text: String) => enBundle.isHashtag(text)

  def sentenceFilter(sentences: Seq[Vector[String]], posMode: POSMode = POSMode.FilterNouns): Seq[Vector[String]] = {
    val filteredSentences: Seq[Vector[String]] = posMode match {
      case mode if mode != POSMode.None => posFilter(sentences, mode)
      case _ => sentences
    }

    filteredSentences
      .filter(_.nonEmpty)
      .filter(sentence => sentence.size > 1 || !Constants.sentenceDelimiter.matcher(sentence.head).matches())
  }

  def posFilter(sentences: Seq[Vector[String]], posMode: POSMode): Seq[Vector[String]] = {
    stanfordTaggedSentences(sentences)
      .map(sentence =>
        sentence
          .filter(tagWord => posMode == POSMode.IncludeNouns  || !tagWord.tag().startsWith("NN") || Constants.sentenceDelimiter.matcher(tagWord.word()).matches())
          .filter(tagWord => !tagWord.tag().startsWith("WP") || !tagWord.tag().startsWith("PRP"))
          .filter(tagWord => !Set("DT", "MD", "CD", "IN", "POS").contains(tagWord.tag()))
          .map(_.word)
      )
  }

  def stanfordTaggedSentences(sentences: Seq[Vector[String]]): Seq[Vector[TaggedWord]] = {
    sentences
      .map(_.map(new Word(_)))
      .map(_.asJava)
      .map(tagger.tagSentence(_).asScala.toVector)
  }
}
