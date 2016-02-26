package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.utils.{Counts, Pos}
import org.hammerlab.pageant.fm.utils.Utils.BlockIdx

import scala.collection.mutable.ArrayBuffer

class BlockIterator(realStartPos: Pos,
                    blockSize: Int,
                    runIter: Iterator[BWTRun]) extends Iterator[(BlockIdx, RunLengthBWTBlock)] {

  var pos = realStartPos.copy()
  var partialRun: BWTRun = _

  override def hasNext: Boolean = {
    runIter.hasNext || partialRun != null
  }

  override def next(): (BlockIdx, RunLengthBWTBlock) = {
    val runs: ArrayBuffer[BWTRun] = ArrayBuffer()

    val startPos = pos.copy()
    val blockIdx = pos.idx / blockSize
    val blockEndIdx = (blockIdx + 1) * blockSize

    def addRun(run: BWTRun): Unit = {
      runs.append(run)
      pos += run
    }

    def takeFromRun(run: BWTRun): Unit = {
      val maxTake = (blockEndIdx - pos.idx).toInt
      if (run.n > maxTake) {
        partialRun = BWTRun(run.t, run.n - maxTake)
        addRun(BWTRun(run.t, maxTake))
      } else {
        addRun(run)
        partialRun = null
      }
    }
    if (partialRun != null) {
      takeFromRun(partialRun)
    }
    while (runIter.hasNext && pos.idx < blockEndIdx) {
      takeFromRun(runIter.next())
    }

    blockIdx → RunLengthBWTBlock(startPos, runs)
  }
}

object BlockIterator {
  def apply(realStartPos: Long,
            realStartCounts: Counts,
            blockSize: Int,
            runIter: Iterator[BWTRun]): BlockIterator =
    new BlockIterator(Pos(realStartPos, realStartCounts), blockSize, runIter)
}
