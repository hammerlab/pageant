package org.hammerlab.pageant.coverage.one_sample.with_intervals

import org.hammerlab.csv.CSVRowI
import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.NumBP
import org.hammerlab.pageant.coverage.one_sample.with_intervals.ResultBuilder.DC
import org.hammerlab.pageant.histogram.JointHistogram.Depth

case class CSVRow(depth: Depth,
                  onBP: NumBP,
                  numOnLoci: NumLoci,
                  fracBPOn: Double,
                  fracLociOn: Double,
                  offBP: NumBP,
                  numOffLoci: NumLoci,
                  fracBPOff: Double,
                  fracLociOff: Double)
  extends CSVRowI

object CSVRow {
  def apply(depthCounts: DC,
            totalBases: NumBP,
            totalOnLoci: NumLoci,
            totalOffLoci: NumLoci): CSVRow = {
    val (depth, Counts(on, off)) = depthCounts
    CSVRow(
      depth,
      on.bp,
      on.n,
      on.bp * 1.0 / totalBases,
      on.n * 1.0 / totalOnLoci,
      off.bp,
      off.n,
      off.bp * 1.0 / totalBases,
      off.n * 1.0 / totalOffLoci
    )
  }
}
