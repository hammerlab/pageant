package org.hammerlab.pageant.fm.index

import org.hammerlab.pageant.fm.blocks.BWTRun
import org.hammerlab.pageant.fm.utils.Utils.{T, Idx}

class IndexedRunLengthIterator(iter: Iterator[(Idx, T)]) extends Iterator[(Idx, BWTRun)] {
  var (idx, cur) = if (!iter.hasNext) (-1: Idx, (-1).toByte) else iter.next()
  var count = 1

  override def hasNext: Boolean = {
    iter.hasNext
  }

  override def next(): (Idx, BWTRun) = {
    var continue = true
    var nextIdx: Idx = 0
    var nextT: T = 0.toByte
    while (continue && iter.hasNext) {
      val p = iter.next()
      nextIdx = p._1
      nextT = p._2
      if (nextT == cur)
        count += 1
      else {
        continue = false
      }
    }
    val r = (idx, BWTRun(cur, count))
    idx = nextIdx
    cur = nextT
    count = 1
    r
  }
}

