package org.hammerlab.pageant.coverage.two_sample.without_intervals

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.NumBP
import org.hammerlab.pageant.coverage.two_sample.Count
import org.hammerlab.pageant.histogram.JointHistogram.Depth

case class CSVRow(depth1: Depth,
                  depth2: Depth,
                  numBP1: NumBP,
                  numBP2: NumBP,
                  numLoci: NumLoci,
                  fracBP1: Double,
                  fracBP2: Double,
                  fracCoveredLoci: Double,
                  fracTotalLoci: Double)

object CSVRow {
  def apply(entry: ((Depth, Depth), Count),
            totalBases1: NumBP,
            totalBases2: NumBP,
            totalCoveredLoci: NumLoci,
            totalLoci: NumLoci): CSVRow = {
    val ((depth1, depth2), Count(bp1, bp2, numLoci)) = entry
    CSVRow(
      depth1,
      depth2,
      bp1,
      bp2,
      numLoci,
      bp1 * 1.0 / totalBases1,
      bp2 * 1.0 / totalBases2,
      numLoci * 1.0 / totalCoveredLoci,
      numLoci * 1.0 / totalLoci
    )
  }
}
