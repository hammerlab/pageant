package org.hammerlab.pageant.coverage.one_sample.without_intervals

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.coverage.one_sample
import org.hammerlab.pageant.coverage.IsKey
import org.hammerlab.pageant.coverage.one_sample.Count
import org.hammerlab.pageant.histogram.JointHistogram.{ Depth, JointHistKey }

case class Key(depth: Depth,
               numLoci: NumLoci)
  extends one_sample.Key[Count] {

  override def toCounts: Count =
    Count(
      depth * numLoci,
      numLoci
    )
}

object Key {
  implicit val isKey =
    new IsKey[Key] {
      override def make(kv: (JointHistKey, NumLoci)): Key = {
        val ((_, depths), count) = kv
        new Key(depths(0).get, count)
      }
    }
}
