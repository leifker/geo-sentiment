package com.github.leifker.spark.sentiment

import java.util.regex.Pattern

import com.peoplepattern.text.Implicits._
import com.peoplepattern.text.LangBundle
import edu.stanford.nlp.ling.{TaggedWord, Word}
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import collection.JavaConverters._

import scala.collection.mutable.{ListBuffer, Buffer => MBuffer}

/**
  * Created by dleifker on 2/16/17.
  */
object NLPUtils {
  lazy val enBundle: LangBundle = LangBundle.bundleForLang(Option("en"))
  lazy val tagger: MaxentTagger = new MaxentTagger("models/english-caseless-left3words-distsim.tagger")

  val isEnglish = (text: String) => text.lang == Some("en")

  val enchancedTokens = (text: String) => text match {
    case in if isEnglish(in) => Option(tokenPreprocess(stringPreprocess(text).tokens))
    case _ => Option.empty
  }

  def tokenPreprocess(tokens: Vector[String]): Vector[String] = {
    Seq(tokens)
      .map(collapsePunctuation)
      .map(posFilter)
      .map(_.filter(word => !enBundle.stopwords(word.toLowerCase)))
      .map(markAllCaps(_, _ + "!").map(_.toLowerCase()))  // mark caps then drop case
      .map(patchToken(_, bothPunctuation, _ + bothPunctuation))
      .map(patchToken(_, "?", _ + "?"))
      .map(patchToken(_, "!", _ + "!"))
      .head
  }

  def stringPreprocess(input: String): String = removeTriplicate(input)

  def posFilter(tokens: Seq[String]): Vector[String] = {
    stanfordTaggedSentences(tokens)
      .map(sentence =>
        sentence.filter(tagWord => !tagWord.tag().startsWith("NN") || punctuationPattern.matcher(tagWord.word()).matches())
          .filter(!_.tag().startsWith("WP"))
          .filter(_.tag() != "DT")
          .map(_.word)
      )
      .filter(_.nonEmpty)
      .filter(sentence => sentence.size > 1 || !punctuationPattern.matcher(sentence.head).matches())
      .flatMap(_.iterator)
      .toVector
  }

  def stanfordTaggedSentences(tokens: Seq[String]): Seq[Seq[TaggedWord]] = {
    sentences(tokens)
      .map(_.map(new Word(_)))
      .map(_.asJava)
      .map(tagger.tagSentence(_).asScala.toSeq)
  }

  def sentences(tokens: Seq[String], includePunctuation: Boolean = true): Seq[Vector[String]] = {
    val seqs = MBuffer.empty[Vector[String]]
    val thisbf = MBuffer.empty[String]
    for (token <- tokens) {
      if (!punctuationPattern.matcher(token).matches()) {
        thisbf += token
      } else {
        if (includePunctuation) {
          thisbf += token
        }
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
    seqs
  }

  def socialTerms(tokens: Seq[String]): Seq[String] = {
    tokens.filter(w => enBundle.isMention(w) || enBundle.isHashtag(w)).distinct
  }

  def patchToken(tokens: Vector[String], punctuationTarget: String, mutator: String => String): Vector[String] = {
    patchTokens(tokens, tokens.length - 1, Seq(punctuationTarget), mutator)
  }

  private def patchTokens(tokens: Vector[String], endIdx: Int, punctuationTarget: Seq[String], mutator: String => String): Vector[String] = {
    val lastIdx = tokens.lastIndexOfSlice(punctuationTarget, endIdx)
    if (lastIdx > 0) {
      val prevPunctIdx = prevPunctuation(tokens, lastIdx - 1)
      val startPatchIdx = prevPunctIdx + punctuationTarget.size
      val nextTokens = lastIdx - startPatchIdx match {
        case diff if diff > 0 => tokens.patch(startPatchIdx, tokens.slice(startPatchIdx, lastIdx).map(mutator), diff)
        case _ => tokens
      }
      patchTokens(nextTokens, prevPunctIdx, punctuationTarget, mutator)
    } else {
      tokens
    }
  }

  private val punctuationPattern = Pattern.compile("""[.?!]+""")
  private def prevPunctuation(tokens: Vector[String], endIdx: Int): Int = tokens.lastIndexWhere(punctuationPattern.matcher(_).matches(), endIdx)

  private def markAllCaps(tokens: Vector[String], mutator: String => String): Vector[String] = tokens.map({
    case str if str.length > 1 && !punctuationPattern.matcher(str).matches() && str == str.toUpperCase => mutator(str)
    case str => str
  })

  private val bothPunctuation = "?!"
  private val collapsable: Set[String]  = Set("!", "?")
  private def collapsePunctuation(input: Vector[String]): Vector[String] = {
    (ListBuffer[String]() /: input){
      case (a, b) if a.nonEmpty && collapsable.contains(b) && (a.last == b || a.last == bothPunctuation) => a
      case (a, b) if a.nonEmpty && collapsable.contains(a.last) && collapsable.contains(b) => a.trimEnd(1); a += bothPunctuation
      case (a, b) => a += b
    }.toVector
  }

  private def removeTriplicate(input: String): String = {
    (new StringBuilder(input.length) /: input.toCharArray){
      case (a, b) if a.size > 2 && a.charAt(a.length - 1) == b && a.charAt(a.length - 2) == b => a
      case (a, b) => a += b
    }.toString
  }
}
