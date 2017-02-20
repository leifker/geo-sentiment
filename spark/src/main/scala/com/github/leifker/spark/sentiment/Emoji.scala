package com.github.leifker.spark.sentiment

import java.util.regex.Pattern

/**
  * Created by dleifker on 2/20/17.
  */
object Emoji {
  private def reOR(parts: String*) = "(" + parts.toList.mkString("|") + ")"
  private val p = {
    val sideways_text_emoji = """>?[:;=]['\-D\)\]\(\[pPdoO/\*3\\]+"""
    val hearts = "<+/?3+" // <3
    val emoji_block0 = "[\u2600-\u27BF]"
    val emoji_block1 = "[\uD83C][\uDF00-\uDFFF]"
    val emoji_block1b = "[\uD83D][\uDC00-\uDE4F]"
    val emoji_block2 = "[\uD83D][\uDE80-\uDEFF]"

    val reParts = reOR(
      sideways_text_emoji,
      emoji_block0,
      emoji_block1,
      emoji_block1b,
      emoji_block2,
      hearts
    )
    Pattern.compile(reParts, Pattern.UNICODE_CHARACTER_CLASS)
  }

  def isEmoji(text: String) = text.length > 1 && p.matcher(text).matches()
}
