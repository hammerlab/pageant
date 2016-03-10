package org.hammerlab.pageant.utils

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Suite}

import scala.collection.mutable.ArrayBuffer

trait BeforeAndAfterTest extends BeforeAndAfterAll {
  self: Suite =>
  var befores: ArrayBuffer[() => Unit] = ArrayBuffer()
  var afters: ArrayBuffer[() => Unit] = ArrayBuffer()
  override def beforeAll() = {
    befores.foreach(_())
  }
  override def afterAll() = {
    afters.foreach(_())
  }
}
