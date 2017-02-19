package org.hammerlab.pageant.coverage.two_sample.without_intervals

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.coverage.{ IsKey, two_sample }
import org.hammerlab.pageant.coverage.two_sample.Count
import org.hammerlab.pageant.histogram.JointHistogram.{ Depth, Depths, OCN }

case class Key(depth1: Depth,
               depth2: Depth,
               numLoci: NumLoci)
 extends two_sample.Key[Count] {

  override def toCounts: Count =
    Count(
      bp1 = depth1 * numLoci,
      bp2 = depth2 * numLoci,
      n = numLoci
    )
}

object Key {
  implicit val isKey =
    new IsKey[Key] {
      override def make(kv: ((OCN, Depths), NumLoci)): Key = {
        val ((_, depths), numLoci) = kv
        new Key(depths(0).get, depths(1).get, numLoci)
      }
    }
}
