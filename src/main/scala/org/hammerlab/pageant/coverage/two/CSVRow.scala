package org.hammerlab.pageant.coverage.two

import org.hammerlab.genomics.reference.NumLoci
import org.hammerlab.pageant.NumBP
import org.hammerlab.pageant.coverage.two.Result.D2C
import org.hammerlab.pageant.histogram.JointHistogram.Depth
import org.hammerlab.csv.CSVRowI

case class CSVRow(depth1: Depth,
                  depth2: Depth,
                  onBP1: NumBP,
                  onBP2: NumBP,
                  numOnLoci: NumLoci,
                  fracBPOn1: Double,
                  fracBPOn2: Double,
                  fracLociOn: Double,
                  offBP1: NumBP,
                  offBP2: NumBP,
                  numOffLoci: NumLoci,
                  fracBPOff1: Double,
                  fracBPOff2: Double,
                  fracLociOff: Double)
  extends CSVRowI

object CSVRow {

  def apply(entry: D2C, totalBases1: NumBP, totalBases2: NumBP, totalIntervalLoci: NumLoci): CSVRow = {
    val ((depth1, depth2), Counts(on, off)) = entry
    CSVRow(
      depth1,
      depth2,
      on.bp1,
      on.bp2,
      on.n,
      on.bp1 * 1.0 / totalBases1,
      on.bp2 * 1.0 / totalBases2,
      on.n * 1.0 / totalIntervalLoci,
      off.bp1,
      off.bp2,
      off.n,
      off.bp1 * 1.0 / totalBases1,
      off.bp2 * 1.0 / totalBases2,
      off.n * 1.0 / totalIntervalLoci
    )
  }
}
