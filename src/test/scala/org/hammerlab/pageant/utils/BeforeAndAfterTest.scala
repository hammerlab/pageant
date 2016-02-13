package org.hammerlab.pageant.utils

import org.scalatest.{BeforeAndAfter, Suite}

import scala.collection.mutable.ArrayBuffer

trait BeforeAndAfterTest extends BeforeAndAfter {
  self: Suite =>
  var befores: ArrayBuffer[() => Unit] = ArrayBuffer()
  var afters: ArrayBuffer[() => Unit] = ArrayBuffer()
  before {
    befores.foreach(_())
  }
  after {
    afters.foreach(_())
  }
}
