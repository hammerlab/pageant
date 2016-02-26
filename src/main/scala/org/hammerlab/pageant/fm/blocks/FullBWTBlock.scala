package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.utils.Utils.{ST, T}
import org.hammerlab.pageant.fm.utils.{Counts, Pos}

case class FullBWTBlock(pos: Pos, data: ST) extends BWTBlock {
  override def toString: String = {
    s"BWTC($startIdx: ${startCounts.mkString(",")}, ${data.mkString(",")})"
  }

  def occ(t: T, v: Long): Long = {
    var count = startCounts(t)
    var i = 0
    while (i + startIdx < v) {
      if (data(i) == t) count += 1
      i += 1
    }
    count
  }
}

object FullBWTBlock {
  def apply(startIdx: Long,
            startCounts: Counts,
            data: Seq[T]): FullBWTBlock =
    FullBWTBlock(Pos(startIdx, startCounts), data.toArray)
}
