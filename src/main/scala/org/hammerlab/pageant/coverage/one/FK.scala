package org.hammerlab.pageant.coverage.one

import org.hammerlab.pageant.histogram.JointHistogram._

case class FK(c: String, d: Depth, on: Long, off: Long)
object FK {
  def make(t: ((OS, Depths), L)): FK = {
    val ((Some(contig), depths), count) = t
    val (on, off) =
      if (depths(1).get == 1)
        (count, 0L)
      else
        (0L, count)
    new FK(contig, depths(0).get, on, off)
  }
}

