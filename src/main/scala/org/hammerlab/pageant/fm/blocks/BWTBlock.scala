package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.index.SparkFM.Counts
import org.hammerlab.pageant.fm.utils.Bound
import org.hammerlab.pageant.fm.utils.Utils.{AT, T, toC}

trait BWTBlock {
  def startIdx: Long
  def startCounts: Counts
  def data: AT

  def occ(t: T, bound: Bound): Long

  override def equals(other: Any): Boolean = {
    other match {
      case b: BWTBlock =>
        startIdx == b.startIdx &&
          startCounts.sameElements(b.startCounts) &&
          data.sameElements(b.data)
      case _ => false
    }
  }
}

case class BWTRun(t: T, n: Int) {
  override def toString: String = {
    s"$n${toC(t)}"
  }
}

