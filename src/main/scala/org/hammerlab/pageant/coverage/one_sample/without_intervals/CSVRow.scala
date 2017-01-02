package org.hammerlab.pageant.coverage.one_sample.without_intervals

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.NumBP
import org.hammerlab.pageant.coverage.one_sample.Count
import org.hammerlab.pageant.coverage.one_sample.without_intervals.ResultBuilder.DC
import org.hammerlab.pageant.histogram.JointHistogram.Depth

case class CSVRow(depth: Depth,
                  numBP: NumBP,
                  numLoci: NumLoci,
                  fracBP: Double,
                  fracCoveredLoci: Double,
                  fracTotalLoci: Double)

object CSVRow {
  def apply(depthCounts: DC,
            totalBases: NumBP,
            totalCoveredLoci: NumLoci,
            totalLoci: NumLoci): CSVRow = {
    val (depth, Count(numBP, numLoci)) = depthCounts
    CSVRow(
      depth,
      numBP,
      numLoci,
      numBP * 1.0 / totalBases,
      numLoci * 1.0 / totalCoveredLoci,
      numLoci * 1.0 / totalLoci
    )
  }
}
