package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.utils.Utils.toI
import org.scalatest.{FunSuite, Matchers}

class BWTRunsIteratorTest extends FunSuite with Matchers {
  def run(c: Char, n: Int): BWTRun = BWTRun(toI(c), n)

  test("empty") {
    val it = new BWTRunsIterator(Array().toIterator)
    it.hasNext should be (false)
    it.toList should be (List())
  }

  test("simple") {
    val it =
      new BWTRunsIterator(
        Array(
          RunLengthBWTBlock(123L, Array(1L, 2L, 3L, 4L, 5L, 6L), List(run('A', 10)))
        ).toIterator
      )

    it.idx should be (123)
    it.counts.c should be (Array(1L, 2L, 3L, 4L, 5L, 6L))
    it.hasNext should be (true)
    it.next should be (run('A', 10))

    it.idx should be (133)
    it.counts.c should be (Array(1L, 12L, 3L, 4L, 5L, 6L))
    it.hasNext should be (false)
  }

  test("multiple") {
    val it =
      new BWTRunsIterator(
        Array(
          RunLengthBWTBlock(123L, Array(1L, 2L, 3L, 4L, 5L, 6L), List(run('A', 10), run('G', 5), run('C', 1)))
        ).toIterator
      )

    it.idx should be (123)
    it.counts.c should be (Array(1L, 2L, 3L, 4L, 5L, 6L))
    it.hasNext should be (true)
    it.next should be (run('A', 10))

    it.idx should be (133)
    it.counts.c should be (Array(1L, 12L, 3L, 4L, 5L, 6L))
    it.hasNext should be (true)
    it.next should be (run('G', 5))

    it.idx should be (138)
    it.counts.c should be (Array(1L, 12L, 3L, 9L, 5L, 6L))
    it.hasNext should be (true)
    it.next should be (run('C', 1))

    it.idx should be (139)
    it.counts.c should be (Array(1L, 12L, 4L, 9L, 5L, 6L))
    it.hasNext should be (false)
  }
}


