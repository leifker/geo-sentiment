package com.github.leifker.spark.sentiment

/**
  * Created by dleifker on 2/23/17.
  */
object TokenPatchUtil {
  private val patchExclaimQuestion = (tokens: Vector[String]) => patchToken(tokens, Constants.exclaimQuestion, _ + Constants.exclaimQuestion)
  private val patchQuestion = (tokens: Vector[String]) => patchToken(tokens, "?", _ + "?")
  private val patchExclaim = (tokens: Vector[String]) => patchToken(tokens, "!", _ + "!")

  val patchWithPunctuation: (Vector[String]) => Vector[String] = patchExclaimQuestion andThen patchExclaim andThen patchQuestion

  private def prevPunctuation(tokens: Vector[String], endIdx: Int): Int = tokens.lastIndexWhere(Constants.sentenceDelimiter.matcher(_).matches(), endIdx)

  private def patchToken(tokens: Vector[String], punctuationTarget: String, mutator: String => String): Vector[String] = {
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
}
