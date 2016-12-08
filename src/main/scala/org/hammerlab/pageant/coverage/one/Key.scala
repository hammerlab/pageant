package org.hammerlab.pageant.coverage.one

import org.hammerlab.genomics.reference.{ ContigName, NumLoci }
import org.hammerlab.pageant.histogram.JointHistogram.{ Depth, Depths }

case class Key(contigName: ContigName, depth: Depth, numLociOn: NumLoci, numLociOff: NumLoci)

object Key {
  def make(t: ((Option[ContigName], Depths), NumLoci)): Key = {
    val ((Some(contig), depths), count) = t

    val (on, off) =
      if (depths(1).get == 1)
        (count, 0L)
      else
        (0L, count)

    new Key(contig, depths(0).get, on, off)
  }
}

