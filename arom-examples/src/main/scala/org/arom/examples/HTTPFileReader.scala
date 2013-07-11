package org.arom.examples

import org.arom.core.Data
import org.arom.core.InputOperator
import org.arom.core.ScalarOperator
import org.arom.core.Locatable
import org.arom.util.Logging

import java.net.URL

/*
 * Naive HTTP file reader. Read the entire file when initialized, and return a
 * bit of the data each time sendData is called
 */

class HTTPFileReader(val path: String) extends InputOperator with Logging {
  def copy = throw new Error("This shouldn't happen")

  var content: Seq[String] = null
  override def init(rt) = {
    log.slf4j debug path
    val url = new URL(path)
    log.slf4j debug "URL: %s".format(url)
    content = url.getContent().toString.split(" ")
  }

  private var _hasData = true
  def hasData = _hasData

  def sendData(rt): Unit = {
    content foreach(t => rt.emit(0, t))
    _hasData = false
  }
}
