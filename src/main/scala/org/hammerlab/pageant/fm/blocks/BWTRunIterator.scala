package org.hammerlab.pageant.fm.blocks

class BWTRunIterator(block: RunLengthBWTBlock) extends Iterator[BWTRun] {
  val it = block.pieces.toIterator
  var counts = block.startCounts.clone()
  var idx = block.startIdx

  override def hasNext: Boolean = {
    it.hasNext
  }

  override def next(): BWTRun = {
    val run = it.next()
    counts(run.t) += run.n
    idx += run.n
    run
  }
}

