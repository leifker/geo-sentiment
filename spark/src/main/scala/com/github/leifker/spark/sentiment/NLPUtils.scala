package com.github.leifker.spark.sentiment

import java.util.regex.Pattern
import com.peoplepattern.text.Implicits._
import scala.collection.mutable.ListBuffer

/**
  * Created by dleifker on 2/16/17.
  */
object NLPUtils {
  val isEnglish = (text: String) => text.lang == Some("en")

  val enchancedTokens = (text: String) => text match {
    case in if isEnglish(in) => Option(tokenPreprocess(stringPreprocess(text).tokens))
    case _ => Option.empty
  }

  def tokenPreprocess(tokens: Vector[String]): Vector[String] = {
    Seq(tokens)
      .map(tokens => markAllCaps(tokens, _ + "!"))
      .map(collapsePunctuation)
      .map(tokens => patchToken(tokens, bothPunctuation, _ + bothPunctuation))
      .map(tokens => patchToken(tokens, "?", _ + "?"))
      .map(tokens => patchToken(tokens, "!", _ + "!"))
      .head
  }

  def stringPreprocess(input: String): String = {
    removeTriplicate(input)
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

  private val punctuationPattern = Pattern.compile("""[.?!]""")
  private def prevPunctuation(tokens: Vector[String], endIdx: Int): Int = tokens.lastIndexWhere(punctuationPattern.matcher(_).matches(), endIdx)

  private def markAllCaps(tokens: Vector[String], mutator: String => String): Vector[String] = tokens.map({
    case str if str.length > 1 && str == str.toUpperCase => mutator(str)
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
