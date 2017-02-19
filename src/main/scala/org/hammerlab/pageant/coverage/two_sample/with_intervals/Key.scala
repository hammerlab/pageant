package org.hammerlab.pageant.coverage.two_sample.with_intervals

import org.hammerlab.genomics.reference.{ ContigName, NumLoci }
import org.hammerlab.pageant.coverage.{ IsKey, two_sample }
import org.hammerlab.pageant.coverage.two_sample.Count
import org.hammerlab.pageant.histogram.JointHistogram._

case class Key(depth1: Depth,
               depth2: Depth,
               numLociOn: NumLoci,
               numLociOff: NumLoci)
  extends two_sample.Key[Counts] {

  override def toCounts: Counts =
    Counts(
      Count(
        depth1 * numLociOn,
        depth2 * numLociOn,
        numLociOn
      ),
      Count(
        depth1 * numLociOff,
        depth2 * numLociOff,
        numLociOff
      )
    )
}

object Key {
  implicit val isKey =
    new IsKey[Key] {
      override def make(kv: ((OCN, Depths), NumLoci)): Key = {
        val ((_, depths), numLoci) = kv

        depths match {
          case Seq(Some(depth1), Some(depth2), Some(intervalDepth)) ⇒
            val (numLociOn, numLociOff) =
              if (depths(2).get == 1)
                (numLoci, NumLoci(0))
              else
                (NumLoci(0), numLoci)

            new Key(depth1, depth2, numLociOn, numLociOff)

          case _ ⇒
            throw new Exception(s"Invalid depths: ${depths.mkString(", ")}")
        }
      }
    }
}
