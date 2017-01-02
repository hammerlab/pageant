package org.hammerlab.pageant.coverage.one_sample.with_intervals

import org.hammerlab.pageant.NumBP
import org.hammerlab.pageant.histogram.JointHistogram.Depth

case class ReadSetStats(maxDepth: Depth, offBases: NumBP, onBases: NumBP) {
  lazy val totalBases = offBases + onBases
}
