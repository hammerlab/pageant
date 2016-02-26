package org.hammerlab.pageant.fm.blocks

import org.scalatest.{FunSuite, Matchers}
import Utils.{runs, counts}
import org.hammerlab.pageant.fm.index.SparkFM.Counts

class BlockIteratorTest extends FunSuite with Matchers {

  test("simple") {
    new BlockIterator(10, counts("1 2 3 2 1 0"), 5, runs("5A 5C 1A 3C 1G").toIterator).toList should be(
      List(
        2 → RunLengthBWTBlock(10, counts("1 2 3 2 1 0"), runs("5A")),
        3 → RunLengthBWTBlock(15, counts("1 7 3 2 1 0"), runs("5C")),
        4 → RunLengthBWTBlock(20, counts("1 7 8 2 1 0"), runs("1A 3C 1G"))
      )
    )
  }

  test("run across block border") {
    new BlockIterator(10, counts("1 2 3 2 1 0"), 5, runs("7A 9C 1A 3C 1G").toIterator).toList should be(
      List(
        2 → RunLengthBWTBlock(10, counts("1  2  3  2  1  0"), runs("5A")),
        3 → RunLengthBWTBlock(15, counts("1  7  3  2  1  0"), runs("2A 3C")),
        4 → RunLengthBWTBlock(20, counts("1  9  6  2  1  0"), runs("5C")),
        5 → RunLengthBWTBlock(25, counts("1  9 11  2  1  0"), runs("1C 1A 3C")),
        6 → RunLengthBWTBlock(30, counts("1 10 15  2  1  0"), runs("1G"))
      )
    )
  }

  test("start off block edge") {
    new BlockIterator(12, counts("1 2 3 2 1 0"), 5, runs("7A 9C 1A 3C 1G").toIterator).toList should be(
      List(
        2 → RunLengthBWTBlock(12, counts("1  2  3  2  1  0"), runs("3A")),
        3 → RunLengthBWTBlock(15, counts("1  5  3  2  1  0"), runs("4A 1C")),
        4 → RunLengthBWTBlock(20, counts("1  9  4  2  1  0"), runs("5C")),
        5 → RunLengthBWTBlock(25, counts("1  9  9  2  1  0"), runs("3C 1A 1C")),
        6 → RunLengthBWTBlock(30, counts("1 10 13  2  1  0"), runs("2C 1G"))
      )
    )
  }
}
