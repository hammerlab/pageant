package org.hammerlab.pageant.fm.index

import org.hammerlab.pageant.fm.blocks.BWTRun
import org.hammerlab.pageant.fm.blocks.Utils.runs
import org.hammerlab.pageant.fm.utils.Utils
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ArrayBuffer

class RunLengthIteratorTest extends FunSuite with Matchers {
  test("first") {
    val it = "CTCTTTTTTTTTAAAAAAAAAAAAC".map(c ⇒ Utils.toI(c)).toIterator
    val rlit = new RunLengthIterator(it)
    val actual = new ArrayBuffer[BWTRun]()
    while (rlit.hasNext) {
      withClue(actual.mkString(" ")) {
        // Verify that repeated hasNext calls are consistent.
        rlit.hasNext should be(true)
      }
      actual.append(rlit.next())
    }
    actual should be(runs("1C 1T 1C 9T 12A 1C"))
  }
}
