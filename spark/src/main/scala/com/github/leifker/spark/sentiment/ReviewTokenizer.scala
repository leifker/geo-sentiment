package com.github.leifker.spark.sentiment

import com.github.leifker.spark.sentiment.POSMode.POSMode
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param._
import com.peoplepattern.text.Implicits._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

import scala.collection.mutable.{ListBuffer, Buffer => MBuffer}

/**
  * Created by dleifker on 2/21/17.
  */
class ReviewTokenizer(override val uid: String) extends UnaryTransformer[String, Seq[String], ReviewTokenizer] with DefaultParamsWritable {
  /**
    * Number of features.  Should be > 0.
    * (default = 3)
    * @group param
    */
  val maxNGram = new IntParam(this, "maxNGrams", "max ngram (> 0)", ParamValidators.gt(0))

  /** @group getParam */
  def getMaxNGram: Int = $(maxNGram)

  /** @group setParam */
  def setMaxNGram(value: Int): this.type = set(maxNGram, value)

  /**
    * Indicates whether ! or ? should be pushed down into the word tokens within sentences
    * Default: true
    * @group param
    */
  val exclaimQuestion: BooleanParam = new BooleanParam(this, "exclaimQuestion", "Apply ! or ? to tokens in sentence")

  /** @group setParam */
  def setExclaimQuestion(value: Boolean): this.type = set(exclaimQuestion, value)

  /** @group getParam */
  def getExclaimQuestion: Boolean = $(exclaimQuestion)

  /**
    * Type of POS filtering to apply
    * Default: FilterNouns
    * @group param
    */
  val posMode: Param[POSMode] = new Param(this, "posMode", "POS method to use")

  /** @group setParam */
  def setPOSMode(value: POSMode): this.type = set(posMode, value)

  /** @group getParam */
  def getPOSMode: POSMode = $(posMode)

  /**
    * Shade grams
    * Default: true
    * @group param
    */
  val shadeGrams: BooleanParam = new BooleanParam(this, "shadeGrams", "Remove grams that are substrings of higher order grams")

  /** @group setParam */
  def setShadeGrams(value: Boolean): this.type = set(shadeGrams, value)

  /** @group getParam */
  def getShadeGrams: Boolean = $(shadeGrams)

  /**
    * Indicates whether ! or ? should be pushed down into the word tokens within sentences
    * Default: true
    * @group param
    */
  val markAllCaps: BooleanParam = new BooleanParam(this, "markAllCaps", "Apply ! when word in all caps")

  /** @group setParam */
  def setMarkAllCaps(value: Boolean): this.type = set(markAllCaps, value)

  /** @group getParam */
  def getMarkAllCaps: Boolean = $(markAllCaps)

  setDefault(maxNGram -> 3, exclaimQuestion -> false, posMode -> POSMode.FilterNouns, shadeGrams -> false, markAllCaps -> true)

  protected override def createTransformFunc: (String) => Seq[String] = text => {
    val tokens = stringPreprocess(text).tokens
    val sentences = NLPUtils.isEnglish(text) match {
      case false => Seq.empty
      case true => processEnhancedTokens(tokens)
    }

    NGramUtils.termNgrams(sentences, 1, getMaxNGram, getShadeGrams) ++ hashTagTokens(tokens) ++ emojiTokens(tokens)
  }

  def this() = this(Identifiable.randomUID("reviewtok"))

  val transform: (String) => Seq[String] = createTransformFunc

  def hashTagTokens(tokens: Vector[String]): Vector[String] = {
    tokens.filter(NLPUtils.isHashtag).map(_.toLowerCase)
  }

  def emojiTokens(tokens: Vector[String]): Vector[String] = {
    tokens.filter(Emoji.isEmoji)
  }

  def processEnhancedTokens(tokens: Vector[String]): Seq[Vector[String]] = {
    val filteredTokens = tokens
      .filter(token => !NLPUtils.isHashtag(token) && !Emoji.isEmoji(token) && !startsWithDigit(token))
      .map(withAndAsDelimiter)

    val stageOne = collapsePunctuation(filteredTokens)

    val stageTwo = (getPOSMode, NLPUtils.sentenceFilter(sentences(stageOne), getPOSMode)) match {
      case (POSMode.FilterNouns, stageTwoTokens) if stageTwoTokens.nonEmpty => stageTwoTokens
      case (POSMode.FilterNouns, _) => NLPUtils.sentenceFilter(sentences(stageOne), POSMode.IncludeNouns)
      case (_, stageTwoTokens) => stageTwoTokens
    }

    val stageThree = stageTwo
      .map(sentence => sentence
          .filter(word => !NLPUtils.enBundle.stopwords(word.toLowerCase))
          .map(markAllCaps(_))
          .map(_.toLowerCase))

    if (getExclaimQuestion) stageThree.map(TokenPatchUtil.patchWithPunctuation) else stageThree
  }

  private val removeTriplicate = (input: String) => {
    (new StringBuilder(input.length) /: input.toCharArray){
      case (a, b) if a.size > 2 && a.charAt(a.length - 1) == b && a.charAt(a.length - 2) == b => a
      case (a, b) => a += b
    }.toString
  }

  private val removeApos: (String) => String = _.replaceAll("'", "")

  private val startsWithDigit = (input: String) => input.headOption.exists(_.isDigit)

  val stringPreprocess: (String) => String = removeTriplicate andThen removeApos

  private def withAndAsDelimiter(input: String): String = input match {
    case word if word.equalsIgnoreCase("and") => "&"
    case word => word
  }

  private def markAllCaps(input: String, mutator: String => String = _ + "!"): String = input match {
    case str if getMarkAllCaps && str.length > 1 && !Constants.allPunctuation.matcher(str).matches() && str == str.toUpperCase => mutator(str)
    case str => str
  }

  def sentences(tokens: Seq[String], includePunctuation: Boolean = true): Seq[Vector[String]] = {
    val seqs = MBuffer.empty[Vector[String]]
    val thisbf = MBuffer.empty[String]
    for (token <- tokens) {
      if (!Constants.sentenceDelimiter.matcher(token).matches()) {
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

  private def collapsePunctuation(input: Vector[String]): Vector[String] = {
    val collapsible: Set[String]  = Set("!", "?")
    (ListBuffer[String]() /: input){
      case (a, b) if a.nonEmpty && collapsible.contains(b) && (a.last == b || a.last == Constants.exclaimQuestion) => a
      case (a, b) if a.nonEmpty && collapsible.contains(a.last) && collapsible.contains(b) => a.trimEnd(1); a += Constants.exclaimQuestion
      case (a, b) => a += b
    }.toVector
  }

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override protected def outputDataType: DataType = new ArrayType(StringType, true)

  override def copy(extra: ParamMap): ReviewTokenizer = defaultCopy(extra)
}

object ReviewTokenizer extends DefaultParamsReadable[ReviewTokenizer] {
  override def load(path: String): ReviewTokenizer = super.load(path)
}