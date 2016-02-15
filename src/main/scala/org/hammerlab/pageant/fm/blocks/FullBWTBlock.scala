package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.index.SparkFM.Counts
import org.hammerlab.pageant.fm.utils.Bound
import org.hammerlab.pageant.fm.utils.Utils.{T, AT}

case class FullBWTBlock(startIdx: Long,
                        startCounts: Counts,
                        data: AT) extends BWTBlock {
  override def toString: String = {
    s"BWTC($startIdx: ${startCounts.mkString(",")}, ${data.mkString(",")})"
  }

  def occ(t: T, bound: Bound): Long = {
    var count = startCounts(t)
    var i = 0
    while (i + startIdx < bound.v) {
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
    FullBWTBlock(startIdx, startCounts, data.toArray)
}
