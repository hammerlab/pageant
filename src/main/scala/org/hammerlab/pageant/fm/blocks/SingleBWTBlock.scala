package org.hammerlab.pageant.fm.blocks

import org.hammerlab.pageant.fm.utils.{Counts, Pos}
import org.hammerlab.pageant.fm.utils.Utils.T

case class SingleBWTBlock(pos: Pos, t: T) extends BWTBlock {
  def data = Array(t)

  override def occ(t: T, v: Long): Long = {
    if (v == startIdx)
      startCounts(t)
    else
      throw new Exception(s"Bad occ call, ($t,$v) at $startIdx")
  }
}
