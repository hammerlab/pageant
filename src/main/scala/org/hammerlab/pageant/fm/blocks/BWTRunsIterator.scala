package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.index.SparkFM.Counts

class BWTRunsIterator(blocks: Iterator[RunLengthBWTBlock]) extends Iterator[BWTRun] {
  var block: RunLengthBWTBlock = _
  var it: BWTRunIterator = _
  var nextRun: BWTRun = _
  var finished = false
  var idx: Long = 0L
  var counts: Counts = _

  def advance(): Boolean = {
    if (it == null || !it.hasNext) {
      if (blocks.hasNext) {
        block = blocks.next()
        it = new BWTRunIterator(block)
        advance()
      } else {
        finished = true
        if (it != null) {
          idx = it.idx
          counts = it.counts.clone
        }
        nextRun = null
        false
      }
    } else {
      idx = it.idx
      counts = it.counts.clone
      nextRun = it.next()
      true
    }
  }

  advance()

  override def hasNext: Boolean = {
    nextRun != null
  }

  override def next(): BWTRun = {
    val r = nextRun
    advance()
    r
  }
}
