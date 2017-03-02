package com.github.leifker.spark.sentiment

import java.util.regex.Pattern

/**
  * Created by dleifker on 2/28/17.
  */
object Constants {
  val allPunctuation = Pattern.compile("""^[\p{Punct}]+""")
  val sentenceDelimiter = Pattern.compile("""[.?!,&#]+""")
  val exclaimQuestion = "?!"
  val htmlEntity = Pattern.compile("""^&\w+;$""")
  val containsDigit = Pattern.compile("""^.*[0-9].*$""")
}
