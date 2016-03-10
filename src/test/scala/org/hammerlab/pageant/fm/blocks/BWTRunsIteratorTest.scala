package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.utils.Utils.toI
import org.scalatest.{FunSuite, Matchers}
import Utils.runs

class BWTRunsIteratorTest extends FunSuite with Matchers {
  def run(c: Char, n: Int): BWTRun = BWTRun(toI(c), n)

  test("empty") {
    val it = BWTRunsIterator(Array().toIterator)
    it.hasNext should be (false)
    it.toList should be (List())
  }

  test("simple") {
    val it =
      BWTRunsIterator(
        Array(
          RunLengthBWTBlock(123L, Array(1L, 2L, 3L, 4L, 5L, 6L), List(run('A', 10)))
        ).toIterator
      )

    it.toArray should be(runs("10A"))
  }

  test("multiple runs") {
    val it =
      BWTRunsIterator(
        Array(
          RunLengthBWTBlock(123L, Array(1L, 2L, 3L, 4L, 5L, 6L), runs("10A 5G 1C"))
        ).toIterator
      )

    it.toArray should be(runs("10A 5G 1C"))
  }

  test("multiple blocks") {
    val it =
      BWTRunsIterator(
        Array(
          RunLengthBWTBlock(123L, Array(1L, 2L, 3L, 4L, 5L, 6L), runs("10A 5G 1C")),
          RunLengthBWTBlock(139L, Array(1L, 12L, 4L, 9L, 5L, 6L), runs("10C 5T 1A")),
          RunLengthBWTBlock(139L, Array(1L, 13L, 14L, 9L, 10L, 6L), runs("1A"))
        ).toIterator
      )

    it.toArray should be(runs("10A 5G 11C 5T 2A"))
  }

  test("repeat then end") {
    val it =
      BWTRunsIterator(
        Array(
          RunLengthBWTBlock(123L, Array(1L, 2L, 3L, 4L, 5L, 6L), runs("10A 5G 1C")),
          RunLengthBWTBlock(139L, Array(1L, 12L, 4L, 9L, 5L, 6L), runs("10C 5T 1A")),
          RunLengthBWTBlock(139L, Array(1L, 13L, 14L, 9L, 10L, 6L), runs("1A 3C"))
        ).toIterator
      )

    it.toArray should be(runs("10A 5G 11C 5T 2A 3C"))
  }

  test("complex runs") {
    val rs =
      runs(
        """
          |2G 3A 2G 1A 1T 3G 1A 1T 1A 1G
          |1A 1G 1A 1T 1A 1G 1C 1T 1C 1A
          |1C 1A 4G 1A 1G 1A 1T 1G 1$ 1C
          |1T 1C 1T 1G 1A 2C 2A 3G 1A 1T
          |4G 1T 5A 1T 2A 2T 2A 2T 4A 1G
          |1A 1G 2A 2T 2A 2C 1A 1C 1G 1C
          |1G 2T 1G 1T 2G 1C 2T 1A
          |""".trim().stripMargin.split("\n").map(_.trim).mkString(" ")
      )
    val it = new BWTRunsIterator(rs.toIterator)
    it.toArray should be(rs)
  }
}


