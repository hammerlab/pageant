package org.hammerlab.pageant.coverage.one_sample.with_intervals

import org.hammerlab.genomics.reference.{ ContigName, NumLoci }
import org.hammerlab.pageant.histogram.JointHistogram.{ Depth, Depths }

case class Key(depth: Depth, numLociOn: NumLoci, numLociOff: NumLoci)

object Key {
  def apply(t: ((Option[ContigName], Depths), NumLoci)): Key = {
    val ((_, depths), count) = t

    val (on, off) =
      if (depths(1).get == 1)
        (count, 0L: NumLoci)
      else
        (0L: NumLoci, count)

    new Key(depths(0).get, on, off)
  }
}

