package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.index.RunLengthIterator
import org.hammerlab.pageant.fm.utils.{Counts, Pos}
import org.hammerlab.pageant.fm.utils.Utils.{AT, T, VT, toC}

case class RunLengthBWTBlock(pos: Pos,
                             pieces: Seq[BWTRun]) extends BWTBlock {
  override def toString: String = {
    s"B(${pos.idx}: ${pos.counts.mkString(",")}, ${pieces.flatMap(p ⇒ Array.fill(p.n)(toC(p.t)).mkString("")).mkString("")} (${pieces.length},${pieces.map(_.n).sum})"
  }

  def lastPos: Pos = pos + pieces

  def data: Seq[T] = pieces.flatMap(p => Array.fill(p.n)(p.t))
  def occ(t: T, v: Long): Long = {
    var count = startCounts(t)
    var pieceIdx = 0
    var idx = startIdx
    while (idx < v && pieceIdx < pieces.length) {
      val piece = pieces(pieceIdx)
      if (piece.t == t) count += piece.n
      idx += piece.n
      pieceIdx += 1
      if (idx > v) {
        if (piece.t == t) count -= (idx - v)
      }
    }
    count
  }
}

object RunLengthBWTBlock {
  def apply(startIdx: Long,
            startCounts: Array[Long],
            pieces: Seq[BWTRun]): RunLengthBWTBlock =
    RunLengthBWTBlock(Pos(startIdx, startCounts), pieces)

  def apply(startIdx: Long,
            startCounts: Counts,
            pieces: Seq[BWTRun]): RunLengthBWTBlock =
    RunLengthBWTBlock(Pos(startIdx, startCounts), pieces)

  def fromTs(startIdx: Long,
             startCounts: Counts,
             data: Seq[T]): RunLengthBWTBlock =
    RunLengthBWTBlock(Pos(startIdx, startCounts), RunLengthIterator(data).toVector)

  def fromTs(startIdx: Long,
             startCounts: Array[Long],
             data: Seq[T]): RunLengthBWTBlock =
    RunLengthBWTBlock(Pos(startIdx, startCounts), RunLengthIterator(data).toVector)
}

