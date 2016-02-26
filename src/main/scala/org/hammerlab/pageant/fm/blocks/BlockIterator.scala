package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.index.SparkFM.Counts
import org.hammerlab.pageant.fm.utils.Utils.BlockIdx

import scala.collection.mutable.ArrayBuffer

class BlockIterator(realStartPos: Long,
                    realStartCounts: Counts,
                    blockSize: Int,
                    runIter: Iterator[BWTRun]) extends Iterator[(BlockIdx, RunLengthBWTBlock)] {

  var idx = realStartPos

  val counts = realStartCounts.clone()

  var partialRun: BWTRun = _

  override def hasNext: Boolean = {
    runIter.hasNext || partialRun != null
  }

  override def next(): (BlockIdx, RunLengthBWTBlock) = {
    val runs: ArrayBuffer[BWTRun] = ArrayBuffer()

    val blockIdx = idx / blockSize
    val startIdx = idx//blockIdx * blockSize
    val blockEndIdx = (blockIdx + 1) * blockSize

    val startCounts: Counts = counts.clone()

    def addRun(run: BWTRun): Unit = {
      runs.append(run)
      idx += run.n
      counts(run.t) += run.n
    }

    def takeFromRun(run: BWTRun): Unit = {
      val maxTake = (blockEndIdx - idx).toInt
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
    while (runIter.hasNext && idx < blockEndIdx) {
      takeFromRun(runIter.next())
    }

    blockIdx → RunLengthBWTBlock(startIdx, startCounts, runs)
  }
}
