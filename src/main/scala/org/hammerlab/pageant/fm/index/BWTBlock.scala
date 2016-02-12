package org.hammerlab.pageant.fm.index

import org.hammerlab.pageant.fm.utils.Bound
import org.hammerlab.pageant.fm.utils.Utils.{T, AT}

case class BWTBlock(startIdx: Long, endIdx: Long, startCounts: Array[Long], data: AT) {
  def occ(t: T, bound: Bound): Long = {
    startCounts(t) + data.take((bound.v - startIdx).toInt).count(_ == t)
  }

  override def toString: String = {
    s"BWTC([$startIdx,$endIdx): ${startCounts.mkString(",")}, ${data.mkString(",")})"
  }

  override def equals(other: Any): Boolean = {
    other match {
      case b: BWTBlock =>
        startIdx == b.startIdx &&
          endIdx == b.endIdx &&
          startCounts.sameElements(b.startCounts) &&
          data.sameElements(b.data)
      case _ => false
    }
  }
}

