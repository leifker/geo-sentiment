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
  val allPunctuation = Pattern.compile("""^[\p{Punct}]+""")
  private val tokenPatcher = TokenPatchUtil()

  lazy val enBundle: LangBundle = LangBundle.bundleForLang(Option("en"))
  lazy val tagger: MaxentTagger = new MaxentTagger("models/english-caseless-left3words-distsim.tagger")

  val isEnglish = (text: String) => text.lang.contains("en")

  def tokenize(text: String) = stringPreprocess(text).tokens

  def enhancedTokens(tokens: Vector[String]) = processEnhancedTokens(tokens)
  def enhancedTokens(text: String): Option[Seq[String]] = if (isEnglish(text)) Option(enhancedTokens(tokenize(text))) else Option.empty

  def hashTagTokens(tokens: Vector[String]): Vector[String] = {
    tokens.filter(enBundle.isHashtag).map(_.toLowerCase)
  }

  def emojiTokens(tokens: Vector[String]): Vector[String] = {
    tokens.filter(Emoji.isEmoji)
  }

  def processEnhancedTokens(tokens: Vector[String]): Vector[String] = {
    val filteredTokens = tokens
      .filter(token => !enBundle.isHashtag(token) && !Emoji.isEmoji(token))
      .map(withAndAsDelimiter)

    val stageOne = collapsePunctuation(filteredTokens)

    val stageTwo = posFilter(stageOne) match {
      case tokens if tokens.nonEmpty => tokens
      case tokens => posFilter(tokens, true)
    }

    val stageThree = stageTwo
      .filter(word => !enBundle.stopwords(word.toLowerCase))
      .map(markAllCaps(_, _ + "!"))
      .map(_.toLowerCase())

    tokenPatcher.patchWithPunctuation(stageThree)
  }

  def stringPreprocess(input: String): String = removeTriplicate(input)

  def posFilter(tokens: Seq[String]): Vector[String] = posFilter(tokens, false)
  def posFilter(tokens: Seq[String], includeNoun: Boolean): Vector[String] = {
    stanfordTaggedSentences(tokens)
      .map(sentence =>
        sentence
          .filter(tagWord => includeNoun || !tagWord.tag().startsWith("NN") || tokenPatcher.sentenceDelimiterPattern.matcher(tagWord.word()).matches())
          .filter(tagWord => !tagWord.tag().startsWith("WP") || !tagWord.tag().startsWith("PRP"))
          .filter(tagWord => !Set("DT", "MD", "CD", "IN", "POS").contains(tagWord.tag()))
          .map(_.word)
      )
      .filter(_.nonEmpty)
      .filter(sentence => sentence.size > 1 || !tokenPatcher.sentenceDelimiterPattern.matcher(sentence.head).matches())
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
      if (!tokenPatcher.sentenceDelimiterPattern.matcher(token).matches()) {
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

  private def withAndAsDelimiter(input: String): String = input match {
    case word if word.equalsIgnoreCase("and") => "&"
    case word => word
  }

  private def markAllCaps(input: String, mutator: String => String): String = input match {
    case str if str.length > 1 && !allPunctuation.matcher(str).matches() && str == str.toUpperCase => mutator(str)
    case str => str
  }

  private val collapsible: Set[String]  = Set("!", "?")
  private def collapsePunctuation(input: Vector[String]): Vector[String] = {
    (ListBuffer[String]() /: input){
      case (a, b) if a.nonEmpty && collapsible.contains(b) && (a.last == b || a.last == tokenPatcher.exclaimQuestion) => a
      case (a, b) if a.nonEmpty && collapsible.contains(a.last) && collapsible.contains(b) => a.trimEnd(1); a += tokenPatcher.exclaimQuestion
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
