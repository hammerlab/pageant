package org.hammerlab.pageant.fm.blocks

class BWTRunsIterator(runs: Iterator[BWTRun]) extends Iterator[BWTRun] {
  var curRun: BWTRun = _
  var nextRun: BWTRun = _
  var finished = false

  def advance() = {
    if (nextRun != null) {
      curRun = nextRun
      nextRun = null
    } else if (runs.hasNext) {
      curRun = runs.next()
    } else {
      curRun = null
    }

    if (curRun != null) {
      var filling = true
      while (runs.hasNext && filling) {
        nextRun = runs.next()
        if (nextRun.t == curRun.t) {
          curRun += nextRun.n
          nextRun = null
        } else {
          filling = false
        }
      }
      if (filling) {
        finished = true
      }
    } else {
      finished = true
    }
  }

  advance()

  override def hasNext: Boolean = {
    !finished || curRun != null
  }

  override def next(): BWTRun = {
    val r = curRun
    advance()
    r
  }
}

object BWTRunsIterator {
  def apply(blocks: Iterator[RunLengthBWTBlock]): BWTRunsIterator = new BWTRunsIterator(blocks.flatMap(_.pieces))
}
