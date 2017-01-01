package org.hammerlab.pageant.coverage.one_sample.with_intervals

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.coverage
import org.hammerlab.pageant.coverage.one_sample.{ Count, IsKey }
import org.hammerlab.pageant.histogram.JointHistogram.{ Depth, JointHistKey }

case class Key(depth: Depth,
               numLociOn: NumLoci,
               numLociOff: NumLoci)
  extends coverage.Key[Counts, Depth] {

  override def toCounts: Counts =
    Counts(
      Count(numLociOn * depth, numLociOn),
      Count(numLociOff * depth, numLociOff)
    )
}

object Key {
  implicit val isKey =
    new IsKey[Key] {
      override def make(kv: (JointHistKey, NumLoci)): Key = {
        val ((_, depths), count) = kv

        val (on, off) =
          if (depths(1).get == 1)
            (count, NumLoci(0))
          else
            (NumLoci(0), count)

        new Key(depths(0).get, on, off)
      }
    }
}

