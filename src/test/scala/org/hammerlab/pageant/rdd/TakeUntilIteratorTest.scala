package org.hammerlab.pageant.rdd

import org.scalatest.{FunSuite, Matchers}

class TakeUntilIteratorTest extends FunSuite with Matchers {
  test("simple") {
    new TakeUntilIterator("abc defg hij".toIterator, ' ').toList.map(_.mkString("")) should be(
      List(
        "abc",
        "bc",
        "c",
        "",
        "defg",
        "efg",
        "fg",
        "g",
        "",
        "hij",
        "ij",
        "j"
      )
    )
  }

  test("double-space and trailing space") {
    new TakeUntilIterator("abc defg  hij ".toIterator, ' ').toList.map(_.mkString("")) should be(
      List(
        "abc",
        "bc",
        "c",
        "",
        "defg",
        "efg",
        "fg",
        "g",
        "",
        "",
        "hij",
        "ij",
        "j",
        ""
      )
    )
  }

  test("leading spaces, multiple-space, trailing spaces") {
    new TakeUntilIterator("  abc defg    hij   ".toIterator, ' ').toList.map(_.mkString("")) should be(
      List(
        "",
        "",
        "abc",
        "bc",
        "c",
        "",
        "defg",
        "efg",
        "fg",
        "g",
        "",
        "",
        "",
        "",
        "hij",
        "ij",
        "j",
        "",
        "",
        ""
      )
    )
  }
}
