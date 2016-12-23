package org.hammerlab.pageant.coverage.two_sample

import org.hammerlab.genomics.reference.{ ContigName, NumLoci }
import org.hammerlab.pageant.histogram.JointHistogram._

case class Key(depth1: Depth, depth2: Depth, numLociOn: NumLoci, numLociOff: NumLoci)

object Key {
  def apply(t: ((Option[ContigName], Depths), NumLoci)): Key = {

    val ((_, depths), numLoci) = t

    depths match {
      case Seq(Some(depth1), Some(depth2), Some(intervalDepth)) ⇒
        val (numLociOn, numLociOff) =
          if (depths(2).get == 1)
            (numLoci, 0L: NumLoci)
          else
            (0L: NumLoci, numLoci)

        new Key(depth1, depth2, numLociOn, numLociOff)

      case _ ⇒
        throw new Exception(s"Invalid depths: ${depths.mkString(", ")}")
    }
  }
}

