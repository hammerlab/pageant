package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.utils.Utils.{AT, T, toC}
import org.hammerlab.pageant.fm.utils.{Bound, Counts, Pos}

trait BWTBlock {
  def pos: Pos
  def startIdx: Long = pos.idx
  def startCounts: Counts = pos.counts
  def data: Seq[T]

  def occ(t: T, bound: Bound): Long = occ(t, bound.v)
  def occ(t: T, v: Long): Long

  override def equals(other: Any): Boolean = {
    other match {
      case b: BWTBlock =>
        startIdx == b.startIdx &&
          startCounts.sameElements(b.startCounts) &&
          data.equals(b.data)
      case _ => false
    }
  }
}

case class BWTRun(t: T, n: Int) {
  override def toString: String = {
    s"$n${toC(t)}"
  }

  def +(more: Int): BWTRun = {
    BWTRun(t, n + more)
  }

}

