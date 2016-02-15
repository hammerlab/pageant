package org.hammerlab.pageant.fm.bwt

import org.hammerlab.pageant.fm.blocks.BWTRun

class DropFirstIncLastIterator(dropFirst: Boolean,
                               incLast: Int,
                               iter: Iterator[BWTRun]) extends Iterator[BWTRun] {
  if (dropFirst) iter.next()

  override def hasNext: Boolean = {
    iter.hasNext
  }

  override def next(): BWTRun = {
    var n = iter.next()
    if (iter.hasNext) {
      n
    } else {
      n.copy(n = n.n + incLast)
    }
  }
}

