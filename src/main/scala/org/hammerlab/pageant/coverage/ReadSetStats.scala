package org.hammerlab.pageant.coverage

import org.hammerlab.pageant.NumBP
import org.hammerlab.pageant.histogram.JointHistogram.Depth

case class ReadSetStats(maxDepth: Depth, totalBases: NumBP, onBases: NumBP) {
  lazy val offBases = totalBases - onBases
}
