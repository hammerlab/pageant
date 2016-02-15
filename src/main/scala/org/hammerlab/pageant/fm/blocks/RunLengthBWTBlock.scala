package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.index.RunLengthIterator
import org.hammerlab.pageant.fm.index.SparkFM.Counts
import org.hammerlab.pageant.fm.utils.Bound
import org.hammerlab.pageant.fm.utils.Utils.{T, AT}

case class RunLengthBWTBlock(startIdx: Long,
                             startCounts: Counts,
                             pieces: Array[BWTRun]) extends BWTBlock {
  override def toString: String = {
    s"B($startIdx: ${startCounts.mkString(",")}, ${pieces.mkString(" ")} (${pieces.length},${pieces.map(_.n).sum})"
  }

  def data: AT = pieces.flatMap(p => Array.fill(p.n)(p.t))
  def occ(t: T, bound: Bound): Long = {
    var count = startCounts(t)
    var pieceIdx = 0
    var idx = startIdx
    while (idx < bound.v && pieceIdx < pieces.length) {
      val piece = pieces(pieceIdx)
      if (piece.t == t) count += piece.n
      idx += piece.n
      pieceIdx += 1
      if (idx > bound.v) {
        if (piece.t == t) count -= (idx - bound.v)
      }
    }
    count
  }
}

object RunLengthBWTBlock {
  def apply(startIdx: Long,
            startCounts: Counts,
            pieces: Seq[BWTRun]): RunLengthBWTBlock =
    RunLengthBWTBlock(startIdx, startCounts, pieces.toArray)

  def fromTs(startIdx: Long,
             startCounts: Counts,
             data: Seq[T]): RunLengthBWTBlock =
    RunLengthBWTBlock(startIdx, startCounts, RunLengthIterator(data).toArray)
}
