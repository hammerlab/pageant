package org.hammerlab.pageant.fm.index

import org.hammerlab.pageant.fm.blocks.BWTRun
import org.hammerlab.pageant.fm.utils.Utils.T

case class RunLengthIterator(iter: Iterator[T]) extends Iterator[BWTRun] {
  var cur: T = if (iter.isEmpty) (-1).toByte else iter.next()
  var count = 1
  var done = false

  override def hasNext: Boolean = {
    if (iter.hasNext)
      true
    else {
      if (!done) {
        done = true
        true
      } else {
        false
      }
    }
  }

  override def next(): BWTRun = {
    if (done) {
      BWTRun(cur, count)
    } else {
      var continue = true
      var n: Byte = 0
      while (continue && iter.hasNext) {
        n = iter.next()
        if (n == cur)
          count += 1
        else {
          continue = false
        }
      }
      if (continue) done = true
      val r = BWTRun(cur, count)
      cur = n
      count = 1
      r
    }
  }
}

object RunLengthIterator {
  def apply(seq: Seq[T]): RunLengthIterator = new RunLengthIterator(seq.toIterator)
}
