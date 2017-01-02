package org.hammerlab.pageant.coverage.one_sample.without_intervals

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.NumBP
import org.hammerlab.pageant.coverage.one_sample.Count
import org.hammerlab.pageant.coverage.one_sample.without_intervals.ResultBuilder.DC
import org.hammerlab.pageant.histogram.JointHistogram.Depth

case class CSVRow(depth: Depth,
                  numBP: NumBP,
                  numLoci: NumLoci)

object CSVRow {
  def apply(depthCounts: DC,
            totalBases: NumBP,
            totalLoci: NumLoci): CSVRow = {
    val (depth, Count(numBP, numLoci)) = depthCounts
    CSVRow(
      depth,
      numBP,
      numLoci
    )
  }
}
